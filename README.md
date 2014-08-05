# Modernize #

Modernize is a tool to build MDS repositories from DSpace repositories. The intention is to
greatly lower the barriers to populating a repository for evaluation/exploration, rather than providing a 
comprehensive tool to migrate from one system to another. Thus, only content objects and metadata
are copied, but not users, resource policies, open workflows, etc. It is non-destructive to the DSpace
repository, but *does* require enough scratch space to have a copy of all the content files.

You may select any arbitrary subset of the DSpace repository to convert, where the subset is defined
by the subtree under a given handle. For example, if you specify a top-level community, it would grab
all the sub-communities and collections under that community, but ignore all sibling top-level
communities. If you specify a collection or non-top-level community, it will grab the direct
parents up to the root, but no siblings.

## Installation ##

Simply drop the built jar into [dspace]/lib, where [dspace] is your installation
(deployment) directory. It is a command-line tool, so Tomcat is not involved.

NB: important limitation! This code requires a Java 7 JRE, regardless of the DSpace version.

## Invocation ##

To run the tool, cd to the [dspace]/bin directory, then type:

    ./dspace dsrun edu.mit.lib.tools.Modernize -i <handle> -s <scratch> -t <target> 

where _handle_ is the community or collection defining the content subset, or 'all' to process the entire repository,
_target_ is the URL of an mds repository where you have administrative privileges,
and _scratch_ is the name of a directory where the tool can write temporary files. Since creating the serialized
version of the DSpace content in the scratch area may not need to be performed more than once, one can in fact
run the command *without* an mds target:

    ./dspace dsrun edu.mit.lib.tools.Modernize -i <handle> -s <scratch> 

This will create SIPs for all the items, collections and communities in the scratch area. Then the command:

    ./dspace dsrun edu.mit.lib.tools.Modernize -s <scratch>  -t <target>

would load them into the target mds repository. One can rerun this command against different targets, since the scratch SIPs
are not deleted.

## Under the Hood ##

The tool operates by creating, for each community, collection, and item in the subtree a Bagit-based SIP package, and then
using the REST API of the mds repository, POSTs these SIPs. It keeps track of the SIPs by creating a simple map (called export.map)
file that shows the relationships among them, so that they can be POSTed to the new repository in the correct order (top community, then
collection, then items, etc). Each SIP is named with it's object's handle, although these are not preserved on the MDS repository.

