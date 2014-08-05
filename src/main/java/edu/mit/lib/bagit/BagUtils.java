/**
 * Copyright 2014 MIT Libraries
 * Licensed under: http://www.apache.org/licenses/LICENSE-2.0
 */
package edu.mit.lib.bagit;

import java.util.Arrays;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

//import com.google.common.hash.HashCode;

import org.dspace.content.DSpaceObject;
import org.dspace.content.DCValue;
import org.dspace.content.Item;
import org.dspace.content.MetadataSchema;
import org.dspace.core.Context;
import org.dspace.curate.Utils;
//import org.dspace.pack.PackingFilter;

// Warning - static import ahead!
import static javax.xml.stream.XMLStreamConstants.*;
import static edu.mit.lib.bagit.Bag.*;

/**
 * BagUtils has a few helper objects for reading from and writing to bags.
 * A Bag is a directory with contents and a little structured metadata in it.
 * 
 * @author richardrodgers
 */
public class BagUtils {

    // basic bag property names - some optional
    public static final String OBJFILE = "object.properties";
    public static final String BAG_TYPE = "bagType";
    public static final String OBJECT_TYPE = "objectType";
    public static final String OBJECT_ID = "objectId";
    public static final String OWNER_ID = "ownerId";
    public static final String OTHER_IDS = "otherIds";
    public static final String CREATE_TS = "created";
    public static final String WITHDRAWN  = "withdrawn";

    static final String ENCODING = "UTF-8";
    static final String CS_ALGO = "MD5";

    private static XMLInputFactory inFactory = XMLInputFactory.newInstance();
    private static XMLOutputFactory outFactory = XMLOutputFactory.newInstance();


    public static FlatReader flatReader(InputStream in) throws IOException {
        return (in != null) ? new FlatReader(in) : null;
    }

    public static XmlReader xmlReader(InputStream in) throws IOException {
        return (in != null) ? new XmlReader(in) : null;
    }

    
    public static FlatWriter flatWriter(String name) throws IOException {
        String brPath = "data/" + name;
        // RLR FIXME
        return new FlatWriter(new File(name), brPath, null);
    }

    public static XmlWriter xmlWriter(OutputStream out) throws IOException {
        return new XmlWriter(out);
    }

    // Assortment of small helper classes for reading & writing bag files
    // Writers capture the checksums of written files, needed for bag manifests

    public static class FlatReader {
        private BufferedReader reader = null;

        private FlatReader(InputStream in) throws IOException {
            reader = new BufferedReader(new InputStreamReader(in));
        }

        public String readLine() throws IOException {
            return reader.readLine();
        }

        public void close() throws IOException {
            reader.close();
        }
    }

    // wrapper for simple Stax-based XML reader
    public static class XmlReader {
        private XMLStreamReader reader = null;

        private XmlReader(InputStream in) throws IOException {
            try {
                reader = inFactory.createXMLStreamReader(in, ENCODING);
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
        }

        public boolean findStanza(String name) throws IOException {
            try {
                while(reader.hasNext()) {
                    if (reader.next() == START_ELEMENT &&
                        reader.getLocalName().equals(name)) {
                        return true;
                    }
                }
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
            return false;
        }

        public Value nextValue() throws IOException {
            Value value = null;
            try {
                while(reader.hasNext()) {
                    switch (reader.next()) {
                        case START_ELEMENT:
                            value = new Value();
                            int numAttrs = reader.getAttributeCount();
                            for (int idx = 0; idx < numAttrs; idx++)  {
                                value.addAttr(reader.getAttributeLocalName(idx),
                                              reader.getAttributeValue(idx));
                            }
                            break;
                        case ATTRIBUTE:
                            break;
                        case CHARACTERS:
                            if (value != null) {
                                value.val = reader.getText();
                            }
                            break;
                        case END_ELEMENT:
                            return value;
                        default:
                            break;
                    }
                }
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
            return value;
        }

        public void close() throws IOException {
            try {
                reader.close();
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
        }
    }

    public static class FlatWriter {
        private String brPath = null;
        private OutputStream out = null;
        private DigestOutputStream dout = null;
        private FlatWriter tailWriter = null;

        private FlatWriter(File file, String brPath, FlatWriter tailWriter) throws IOException {
            try {
                out = new FileOutputStream(file);
                dout = new DigestOutputStream(out, MessageDigest.getInstance(CS_ALGO));
                this.brPath = (brPath != null) ? brPath : file.getName();
                this.tailWriter = tailWriter;
            } catch (NoSuchAlgorithmException nsae) {
                throw new IOException("no such algorithm: " + CS_ALGO);
            }
        }

        public void writeProperty(String key, String value) throws IOException {
            writeLine(key + " " + value);
        }

        public void writeLine(String line) throws IOException {
            byte[] bytes = (line + "\n").getBytes(ENCODING);
            dout.write(bytes);
        }

        public void close() throws IOException {
            dout.flush();
            out.close();
            if (tailWriter != null) {
                tailWriter.writeProperty(
                        // HashCode.fromBytes(dout.getMessageDigest().digest()).toString(),
                        // brPath);
                       Utils.toHex(dout.getMessageDigest().digest()), brPath);
            }
        }
    }

    // Wrapper for simple Stax-based writer
    public static class XmlWriter {
        private OutputStream out = null;
        private XMLStreamWriter writer = null;

        private XmlWriter(OutputStream out) throws IOException {
            try {
                this.out = out;
                writer = outFactory.createXMLStreamWriter(out, ENCODING);
                writer.writeStartDocument(ENCODING, "1.0");
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
        }

        public void startStanza(String name) throws IOException {
            try {
                writer.writeStartElement(name);
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
        }

        public void endStanza() throws IOException {
            try {
                writer.writeEndElement();
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
        }

        public void writeValue(String name, String val) throws IOException {
            if (name != null && val != null) {
                try {
                    writer.writeStartElement("value");
                    writer.writeAttribute("name", name);
                    writer.writeCharacters(val);
                    writer.writeEndElement();
                } catch (XMLStreamException xsE) {
                    throw new IOException(xsE.getMessage(), xsE);
                }
            }
        }

        public void writeValue(Value value) throws IOException {
            try  {
                writer.writeStartElement("value");
                for (String attrName : value.attrs.keySet())
                {
                    String attrVal = value.attrs.get(attrName);
                    if (attrVal != null)
                    {
                        writer.writeAttribute(attrName, attrVal);
                    }
                }
                writer.writeCharacters(value.val);
                writer.writeEndElement();
            } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
            }
        }

        public void close() throws IOException {
            try {
                writer.writeEndDocument();
                writer.flush();
                writer.close();
                out.close();
           } catch (XMLStreamException xsE) {
                throw new IOException(xsE.getMessage(), xsE);
           }
        }
    }

    public static class Value {
        public String name = null;
        public String val = null;
        public Map<String, String> attrs = null;

        public void addAttr(String name, String val) {
            if ("name".equals(name)) {
                this.name = val;
            } else {
                if (attrs == null) {
                    attrs = new HashMap<String, String>();
                }
                attrs.put(name, val);
            }
        }
    }

    public static void writeMetadata(DSpaceObject dso, /*PackingFilter filter,*/ OutputStream out) throws IOException, SQLException {
        XmlWriter writer = xmlWriter(out);
        writer.startStanza("metadata");
        // are we metadata filtering via view or set? (only set supported here)
        String view = null; //filter.getMdViewName();
        if (view == null) {
            Context ctx = null;
            try {
                ctx = new Context();
                for (MetadataSchema schema : MetadataSchema.findAll(ctx)) {
                    //if (filter.acceptMdSet(schema.getName())) {
                        Value value = new Value();
                        DCValue[] vals = ((Item)dso).getMetadata(schema.getName(), Item.ANY, Item.ANY, Item.ANY);
                        for (DCValue val : vals) {
                            value.addAttr("schema", val.schema);
                            value.addAttr("element", val.element);
                            value.addAttr("qualifier", val.qualifier);
                            value.addAttr("language", val.language);
                            value.val = val.value;
                            writer.writeValue(value);
                        }
                    //}
                }
            } finally {
                ctx.abort();
            }
        }
        writer.endStanza();
        writer.close();
    }

   /*
    public static void readMetadata(DSpaceObject dso, InputStream in) throws IOException {
        XmlReader reader = xmlReader(in);
        if (reader != null && reader.findStanza("metadata")) {
            Value value = null;
            while((value = reader.nextValue()) != null) {
                dso.addMetadata(value.attrs.get("schema"),
                                 value.attrs.get("element"),
                                 value.attrs.get("qualifier"),
                                 value.attrs.get("language"),
                                 value.val);
            }
            reader.close();
        }
    }
    */
}
