/**
 * Copyright 2014 MIT Libraries
 * Licensed under: http://www.apache.org/licenses/LICENSE-2.0
 */
package edu.mit.lib.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.PathMatcher;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.Stack;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import static javax.xml.stream.XMLStreamConstants.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;

import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.Collection;
import org.dspace.content.Community;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.ItemIterator;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.handle.HandleManager;
import org.dspace.app.itemexport.ItemExport;

import edu.mit.lib.bagit.Bag;
import edu.mit.lib.bagit.Filler;

import edu.mit.lib.bagit.BagUtils;
import static edu.mit.lib.bagit.BagUtils.*;

/**
 * Class migrates repository data from a DSpace
 * repository to a remote MDS repository. Fairly fast & dirty.
 * @author richardrodgers
 */

public class Modernize {

    private Context context;
    private Path scratchDir;
    private ExportManifest manif;

    public Modernize(Path scratchDir) throws Exception {
        this.scratchDir = scratchDir;
        context = new Context();
        manif = new ExportManifest();
    }

    public static void main(String[] args) throws Exception {

        // create an options object and populate it
        CommandLineParser parser = new PosixParser();

        Options options = new Options();

        options.addOption("i", "identifier", true, "root handle to migrate - 'all' for entire repo");
        options.addOption("t", "target", true, "URL of mds repository to import into");
        options.addOption("s", "scratch", true, "scratch directory for processing");
        options.addOption("m", "migrate", false, "export for migration (remove handle and metadata that will be re-created in new system)");
        options.addOption("h", "help", false, "help");

        CommandLine line = parser.parse(options, args);

        if (line.hasOption('h')) {
            HelpFormatter myhelp = new HelpFormatter();
            myhelp.printHelp("Modernize\n", options);
            System.out
                    .println("\nentire repository: Modernize -i all -t http://my-mds-repo.org/webapi -s /dspace/export");
            System.out
                    .println("\ncontent subtree: Modernize -i 123456789/1 -t http://my-mds-repo.org/webapi -s /dspace/export");
            System.exit(0);
        }

        String scratch = null;
        if (line.hasOption('s')) {
            scratch = line.getOptionValue('s');
            if (scratch == null) {
                System.out.println("Scratch directory required!");
                System.exit(1);
            }
        }
        
        Modernize mod = new Modernize(Paths.get(scratch));

        if (line.hasOption('i')) {
            String id = line.getOptionValue('i');
            if (id != null) {
                mod.exportIdentifier(id);
            } else {
                mod.bail("Must provide an identifer!");
            }
        }

        if (line.hasOption('t')) {
            String targetUrl = line.getOptionValue('t');
            if (targetUrl != null) {
                mod.importToMds(targetUrl);
            } else {
                mod.bail("Must provide an URL to an mds repository!");
            }
        }

        mod.finish();
    }

    private void exportIdentifier(String id) throws IOException, SQLException, AuthorizeException {
        // validate the identifier
        if ( ! "all".equals(id)) {
            DSpaceObject dso = HandleManager.resolveToObject(context, id);
            if (dso == null) {
                bail("Unresolvable identifier: " + id);
            }
            // construct the manifest
            if (dso.getType() == Constants.COMMUNITY) {
                communityManifest((Community)dso);
            } else if (dso.getType() == Constants.COLLECTION) {
                collectionManifest((Collection)dso);
            } else {
                bail("Identifier: " + id + " is not a collection or community");
            }
        } else {
            repoManifest();
        }
        manifestToScratch();
        // flush manifest to disk fro possible future use
        manif.write();
    }

    private void bail(String message) throws SQLException {
        System.out.println(message);
        finish();
        System.exit(1);
    }

    private void repoManifest() throws IOException, SQLException {
        for (Community topComm: Community.findAllTop(context)) {
            communityManifest(topComm);
        }
    }

    private void communityManifest(Community comm) throws IOException, SQLException {
        Stack<Community> parents = new Stack<>();
        Community parent = comm.getParentCommunity();
        while (parent != null) {
            parents.push(parent);
            parent = parent.getParentCommunity();
        }
        int level = manif.addParents(parents);
        manif.addCommunity(comm, level);
    }

    private void collectionManifest(Collection coll) throws IOException, SQLException {
        Stack<Community> parents = new Stack<>();
        Community parent = (Community)coll.getParentObject();
        while (parent != null) {
            parents.push(parent);
            parent = parent.getParentCommunity();
        }
        int level = manif.addParents(parents);
        manif.addCollection(coll, level);
    }

    public void manifestToScratch() throws IOException, SQLException, AuthorizeException {
        // Just create a SIP package for each line in manifest and put in scratch directory
        if (manif.isEmpty()) {
            manif.read();
        }
        for (String handle : manif.entries) {
            DSpaceObject dso = HandleManager.resolveToObject(context, handle);
            if (dso == null) {
                bail("Unresolvable identifier: " + handle);
            }
            switch (dso.getType()) {
                case Constants.COMMUNITY: makeCommPackage((Community)dso);
                case Constants.COLLECTION: makeCollPackage((Collection)dso);
                case Constants.ITEM: makeItemPackage((Item)dso);
            }
        }
    }

    public void importToMds(String targetUrl) throws IOException {
        if (manif.isEmpty()) {
            manif.read();
        }
        Stack<String> parents = new Stack<>();
        parents.push(null);  // indicates no parent object

        for (int i = 0; i < manif.entries.size(); i++) {
            String handle = manif.entries.get(i);
            uploadPackage(getPackage(handle), getPostUrl(targetUrl, parents.peek(), manif.ctypes.get(i)));
            if (i < manif.entries.size() - 1) {
                int diff = manif.levels.get(i) - manif.levels.get(i+1);
                if (diff < 0) {
                    // I have kids - put myself on the parents stack
                    parents.push(handle);
                } else if (diff > 0) {
                    // expose grandparents
                    while (diff-- > 0) {
                        parents.pop();
                    }
                } // if diff == 0 - next entry is a sibling, nothing to do
            }
        }
    }

    private String getPostUrl(String targetUrl, String handle, int ctype) {
        // NB: these URLs are a bit fragile - really should be queried from REST API
        String baseUrl = targetUrl;
        if (! baseUrl.endsWith("/")) {
            baseUrl += "/";
        }
        String pkgName = "package/" + Constants.typeText[ctype] + "-sip";
        return (handle != null) ? baseUrl + handle + pkgName: baseUrl + pkgName;
    }

    private Path getPackage(String handle) {
        return scratchDir.resolve(handle.replaceAll("/", "-") + ".zip");
    }

    private static final String[] commFields = {
        "name",
        "short_description",
        "introductory_text",
        "copyright_text",
        "side_bar_text"
    };

    private Path makeCommPackage(Community comm) throws IOException, SQLException, AuthorizeException {
        Filler filler = new Filler(scratchDir.resolve(comm.getHandle().replaceAll("/", "-")));
        filler.metadata(BAG_TYPE, "SIP");
        filler.property("data/object", OBJECT_TYPE, "community");
        filler.property("data/object", OBJECT_ID, comm.getHandle());
        Community parent = comm.getParentCommunity();
        if (parent != null) {
            filler.property("data/object", OWNER_ID, parent.getHandle());
        }
        // metadata
        OutputStream metaOut = filler.payloadStream("metadata.xml");
        XmlWriter writer = xmlWriter(metaOut);
        writer.startStanza("metadata");
        for (String field : commFields) {
            String val = comm.getMetadata(field);
            if (val != null) {
                writer.writeValue(field, val);
            }
        }
        writer.endStanza();
        writer.close();
        // check for logo
        Bitstream logo = comm.getLogo();
        if (logo != null) {
            filler.payload("logo", logo.retrieve());
        }
        return filler.toPackage();
    }

    private static final String[] collFields = {
        "name",
        "short_description",
        "introductory_text",
        "provenance_description",
        "license",
        "copyright_text",
        "side_bar_text"
    };

    private Path makeCollPackage(Collection coll) throws IOException, SQLException, AuthorizeException {
        Filler filler = new Filler(scratchDir.resolve(coll.getHandle().replaceAll("/", "-")));
        filler.metadata(BAG_TYPE, "SIP");
        filler.property("data/object", OBJECT_TYPE, "collection");
        filler.property("data/object", OBJECT_ID, coll.getHandle());
        DSpaceObject parent = coll.getParentObject();
        if (parent != null) {
            filler.property("data/object", OWNER_ID, parent.getHandle());
        }
         // metadata
        OutputStream metaOut = filler.payloadStream("metadata.xml");
        XmlWriter writer = xmlWriter(metaOut);
        writer.startStanza("metadata");
        for (String field : collFields) {
            String val = coll.getMetadata(field);
            if (val != null) {
                writer.writeValue(field, val);
            }
        }
        writer.endStanza();
        writer.close();
         // check for logo
        Bitstream logo = coll.getLogo();
        if (logo != null) {
            filler.payload("logo", logo.retrieve());
        }
        return filler.toPackage();
    }

    private Path makeItemPackage(Item item) throws IOException, SQLException, AuthorizeException {
        Filler filler = new Filler(scratchDir.resolve(item.getHandle().replaceAll("/", "-")));
        filler.metadata(BAG_TYPE, "SIP");
        filler.property("data/object", OBJECT_TYPE, "item");
        filler.property("data/object", OBJECT_ID, item.getHandle());
        // get collections
        StringBuilder linked = new StringBuilder();
        for (Collection coll : item.getCollections()) {
            if (item.isOwningCollection(coll)) {
                filler.property("data/object", OWNER_ID, coll.getHandle());
            } else {
                linked.append(coll.getHandle()).append(",");
            }
        }
        String linkedStr = linked.toString();
        if (linkedStr.length() > 0) {
            filler.property("data/object", OTHER_IDS, linkedStr.substring(0, linkedStr.length() - 2));
        }
        if (item.isWithdrawn()) {
            filler.property("data/object", WITHDRAWN, "true");
        }
        // metadata
        BagUtils.writeMetadata(item, filler.payloadStream("metadata.xml"));
        // proceed to bundles, in sub-directories, excluding bundles with derivatives
        for (Bundle bundle : item.getBundles()) {
            if (! "TEXT".equals(bundle.getName())) {
                // only bundle metadata is the primary bitstream - remember it
                // and place in bitstream metadata if defined
                int primaryId = bundle.getPrimaryBitstreamID();
                for (Bitstream bs : bundle.getBitstreams()) {
                    // write metadata to xml file
                    String seqId = String.valueOf(bs.getSequenceID());
                    String relPath = bundle.getName() + "/";
                    OutputStream metaOut = filler.payloadStream(relPath + seqId + "-metadata.xml");
                    XmlWriter writer = xmlWriter(metaOut);
                    writer.startStanza("metadata");
                    // field access is hard-coded in Bitstream class, ugh!
                    writer.writeValue("name", bs.getName());
                    writer.writeValue("source", bs.getSource());
                    writer.writeValue("description", bs.getDescription());
                    writer.writeValue("sequence_id", seqId);
                    if (bs.getID() == primaryId) {
                       writer.writeValue("bundle_primary", "true"); 
                    }
                    writer.endStanza();
                    writer.close();
                    // add bytes to bag
                    filler.payload(relPath + seqId, bs.retrieve());
                }
            }
        }
        return filler.toPackage();
    }

    private void uploadPackage(Path pkg, String targetUri) throws IOException {
        // using older Apache http client library to make compatible with more systems
        PostMethod post = new PostMethod(targetUri);
        HttpClient client = new HttpClient();
        RequestEntity entity = new FileRequestEntity(pkg.toFile(), "application/zip");
        post.setRequestEntity(entity);
        try {
            int result = client.executeMethod(post);
        } finally {
            post.releaseConnection();
        }
    }

    private void finish() throws SQLException {
        context.abort();
    }

    // map of content subtree - serialized as YAML file
    private class ExportManifest {

        List<String> entries = new ArrayList<>();
        List<Integer> ctypes = new ArrayList<>();
        List<Integer> levels = new ArrayList<>();
        Path exportMap;

        public ExportManifest() throws IOException {
            exportMap = scratchDir.resolve("export.map");
        }

        public boolean isEmpty() {
            return (entries.size() == 0);
        }

        public int addParents(Stack<Community> parents) throws IOException {
            int level = 0;
            if (! parents.empty()) {
                Community parent = parents.pop();
                while (parent != null) {
                    addHandle(parent.getHandle(), level++, Constants.COMMUNITY);
                    parent = parents.pop();
                }
            }
            return level;
        }

        public void addCommunity(Community comm, int level) throws IOException, SQLException {
            addHandle(comm.getHandle(), level, Constants.COMMUNITY);
            for (Community subComm : comm.getSubcommunities()) {
                addCommunity(subComm, level + 1);
            }
            for (Collection coll : comm.getCollections()) {
                addCollection(coll, level + 1);
            }
        }

        public void addCollection(Collection coll, int level) throws IOException, SQLException {
            addHandle(coll.getHandle(), level, Constants.COLLECTION);
            ItemIterator iiter = coll.getItems();
            while(iiter.hasNext()) {
                addHandle(iiter.next().getHandle(), level + 1, Constants.ITEM);
            }
            iiter.close();
        }

        public void write() throws IOException {
            try (BufferedWriter writer = Files.newBufferedWriter(exportMap, StandardCharsets.UTF_8)) {
                int lineNo = 0;
                for (String entry : entries) {
                    writer.write(levels.get(lineNo));
                    writer.write(" ");
                    writer.write(ctypes.get(lineNo));
                    writer.write(" ");
                    writer.write(entry);
                    writer.newLine();
                    ++lineNo;
                }
            }
        }

        public void read() throws IOException {
            try (Scanner scanner = new Scanner(exportMap, StandardCharsets.UTF_8.name())) {
                while (scanner.hasNextLine()) {
                    levels.add(scanner.nextInt());
                    ctypes.add(scanner.nextInt());
                    entries.add(scanner.next());
                    scanner.nextLine();
                }
            }
        }

        private void addHandle(String handle, int level, int ctype) throws IOException {
            levels.add(level);
            ctypes.add(ctype);
            entries.add(handle);
        }
    }
}
