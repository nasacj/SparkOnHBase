package com.nasacj.spark.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.JarFinder;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class HBaseSecurity implements Serializable{

    static Log LOG = LogFactory.getLog(HBaseSecurity.class);
    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table, Scan scan,
                                          Class<? extends TableMapper> mapper,
                                          Class<?> outputKeyClass,
                                          Class<?> outputValueClass, Job job)
            throws IOException {
        initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass,
                job, true);
    }

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table The table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table, Scan scan,
                                          Class<? extends TableMapper> mapper,
                                          Class<?> outputKeyClass,
                                          Class<?> outputValueClass, Job job,
                                          boolean addDependencyJars)
            throws IOException {
        initTableMapperJob(table, scan, mapper, outputKeyClass,
                outputValueClass, job, addDependencyJars, TableInputFormat.class);
    }


    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table, Scan scan,
                                          Class<? extends TableMapper> mapper,
                                          Class<?> outputKeyClass,
                                          Class<?> outputValueClass, Job job,
                                          boolean addDependencyJars, Class<? extends InputFormat> inputFormatClass)
            throws IOException {
        initTableMapperJob(table, scan, mapper, outputKeyClass, outputValueClass, job,
                addDependencyJars, true, inputFormatClass);
    }

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @param initCredentials whether to initialize hbase auth credentials for the job
     * @param inputFormatClass the input format
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table, Scan scan,
                                          Class<? extends TableMapper> mapper,
                                          Class<?> outputKeyClass,
                                          Class<?> outputValueClass, Job job,
                                          boolean addDependencyJars, boolean initCredentials,
                                          Class<? extends InputFormat> inputFormatClass)
            throws IOException {
        job.setInputFormatClass(inputFormatClass);
        if (outputValueClass != null) job.setMapOutputValueClass(outputValueClass);
        if (outputKeyClass != null) job.setMapOutputKeyClass(outputKeyClass);
        job.setMapperClass(mapper);
        if (Put.class.equals(outputValueClass)) {
            job.setCombinerClass(PutCombiner.class);
        }
        Configuration conf = job.getConfiguration();
        HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
        conf.set(TableInputFormat.INPUT_TABLE, table);
        conf.set(TableInputFormat.SCAN, convertScanToString(scan));
        conf.setStrings("io.serializations", conf.get("io.serializations"),
                MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                KeyValueSerialization.class.getName());
        if (addDependencyJars) {
            addDependencyJars(job);
        }
        if (initCredentials) {
            initCredentials(job);
        }
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan  The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    /**
     * Add the HBase dependency jars as well as jars for any of the configured
     * job classes to the job configuration, so that JobClient will ship them
     * to the cluster and add them to the DistributedCache.
     */
    public static void addDependencyJars(Job job) throws IOException {
        addHBaseDependencyJars(job.getConfiguration());
        try {
            addDependencyJars(job.getConfiguration(),
                    // when making changes here, consider also mapred.TableMapReduceUtil
                    // pull job classes
                    job.getMapOutputKeyClass(),
                    job.getMapOutputValueClass(),
                    job.getInputFormatClass(),
                    job.getOutputKeyClass(),
                    job.getOutputValueClass(),
                    job.getOutputFormatClass(),
                    job.getPartitionerClass(),
                    job.getCombinerClass());
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /**
     * Add HBase and its dependencies (only) to the job configuration.
     * <p>
     * This is intended as a low-level API, facilitating code reuse between this
     * class and its mapred counterpart. It also of use to extenral tools that
     * need to build a MapReduce job that interacts with HBase but want
     * fine-grained control over the jars shipped to the cluster.
     * </p>
     * @param conf The Configuration object to extend with dependencies.
     * @see org.apache.hadoop.hbase.mapred.TableMapReduceUtil
     * @see <a href="https://issues.apache.org/jira/browse/PIG-3285">PIG-3285</a>
     */
    public static void addHBaseDependencyJars(Configuration conf) throws IOException {
        addDependencyJars(conf,
                // explicitly pull a class from each module
                org.apache.hadoop.hbase.HConstants.class,                      // hbase-common
                ClientProtos.class, // hbase-protocol
                Put.class,                      // hbase-client
                org.apache.hadoop.hbase.CompatibilityFactory.class,            // hbase-hadoop-compat
                TableMapper.class,           // hbase-server
                // pull necessary dependencies
                org.apache.zookeeper.ZooKeeper.class,
                org.jboss.netty.channel.ChannelFactory.class,
                com.google.protobuf.Message.class,
                com.google.common.collect.Lists.class,
                org.cloudera.htrace.Trace.class,
                org.cliffc.high_scale_lib.Counter.class); // needed for mapred over snapshots
    }

    /**
     * Add the jars containing the given classes to the job's configuration
     * such that JobClient will ship them to the cluster and add them to
     * the DistributedCache.
     */
    public static void addDependencyJars(Configuration conf,
                                         Class<?>... classes) throws IOException {

        FileSystem localFs = FileSystem.getLocal(conf);
        Set<String> jars = new HashSet<String>();
        // Add jars that are already in the tmpjars variable
        jars.addAll(conf.getStringCollection("tmpjars"));

        // add jars as we find them to a map of contents jar name so that we can avoid
        // creating new jars for classes that have already been packaged.
        Map<String, String> packagedClasses = new HashMap<String, String>();

        // Add jars containing the specified classes
        for (Class<?> clazz : classes) {
            if (clazz == null) continue;

            Path path = findOrCreateJar(clazz, localFs, packagedClasses);
            if (path == null) {
                LOG.warn("Could not find jar for class " + clazz +
                        " in order to ship it to the cluster.");
                continue;
            }
            if (!localFs.exists(path)) {
                LOG.warn("Could not validate jar file " + path + " for class "
                        + clazz);
                continue;
            }
            jars.add(path.toString());
        }
        if (jars.isEmpty()) return;

        conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
    }

    /**
     * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds
     * the Jar for a class or creates it if it doesn't exist. If the class is in
     * a directory in the classpath, it creates a Jar on the fly with the
     * contents of the directory and returns the path to that Jar. If a Jar is
     * created, it is created in the system temporary directory. Otherwise,
     * returns an existing jar that contains a class of the same name. Maintains
     * a mapping from jar contents to the tmp jar created.
     * @param my_class the class to find.
     * @param fs the FileSystem with which to qualify the returned path.
     * @param packagedClasses a map of class name to path.
     * @return a jar file that contains the class.
     * @throws IOException
     */
    private static Path findOrCreateJar(Class<?> my_class, FileSystem fs,
                                        Map<String, String> packagedClasses)
            throws IOException {
        // attempt to locate an existing jar for the class.
        String jar = findContainingJar(my_class, packagedClasses);
        if (null == jar || jar.isEmpty()) {
            jar = getJar(my_class);
            updateMap(jar, packagedClasses);
        }

        if (null == jar || jar.isEmpty()) {
            return null;
        }

        LOG.debug(String.format("For class %s, using jar %s", my_class.getName(), jar));
        return new Path(jar).makeQualified(fs);
    }

    /**
     * Invoke 'getJar' on a JarFinder implementation. Useful for some job
     * configuration contexts (HBASE-8140) and also for testing on MRv2. First
     * check if we have HADOOP-9426. Lacking that, fall back to the backport.
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     */
    private static String getJar(Class<?> my_class) {
        String ret = null;
        String hadoopJarFinder = "org.apache.hadoop.util.JarFinder";
        Class<?> jarFinder = null;
        try {
            LOG.debug("Looking for " + hadoopJarFinder + ".");
            jarFinder = Class.forName(hadoopJarFinder);
            LOG.debug(hadoopJarFinder + " found.");
            Method getJar = jarFinder.getMethod("getJar", Class.class);
            ret = (String) getJar.invoke(null, my_class);
        } catch (ClassNotFoundException e) {
            LOG.debug("Using backported JarFinder.");
            ret = JarFinder.getJar(my_class);
        } catch (InvocationTargetException e) {
            // function was properly called, but threw it's own exception. Unwrap it
            // and pass it on.
            throw new RuntimeException(e.getCause());
        } catch (Exception e) {
            // toss all other exceptions, related to reflection failure
            throw new RuntimeException("getJar invocation failed.", e);
        }

        return ret;
    }


    /**
     * Add entries to <code>packagedClasses</code> corresponding to class files
     * contained in <code>jar</code>.
     * @param jar The jar who's content to list.
     * @param packagedClasses map[class -> jar]
     */
    private static void updateMap(String jar, Map<String, String> packagedClasses) throws IOException {
        if (null == jar || jar.isEmpty()) {
            return;
        }
        ZipFile zip = null;
        try {
            zip = new ZipFile(jar);
            for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter.hasMoreElements();) {
                ZipEntry entry = iter.nextElement();
                if (entry.getName().endsWith("class")) {
                    packagedClasses.put(entry.getName(), jar);
                }
            }
        } finally {
            if (null != zip) zip.close();
        }
    }

    /**
     * Find a jar that contains a class of the same name, if any. It will return
     * a jar file, even if that is not the first thing on the class path that
     * has a class with the same name. Looks first on the classpath and then in
     * the <code>packagedClasses</code> map.
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     */
    private static String findContainingJar(Class<?> my_class, Map<String, String> packagedClasses)
            throws IOException {
        ClassLoader loader = my_class.getClassLoader();
        String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";

        // first search the classpath
        for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements(); ) {
            URL url = itr.nextElement();
            if ("jar".equals(url.getProtocol())) {
                String toReturn = url.getPath();
                if (toReturn.startsWith("file:")) {
                    toReturn = toReturn.substring("file:".length());
                }
                // URLDecoder is a misnamed class, since it actually decodes
                // x-www-form-urlencoded MIME type rather than actual
                // URL encoding (which the file path has). Therefore it would
                // decode +s to ' 's which is incorrect (spaces are actually
                // either unencoded or encoded as "%20"). Replace +s first, so
                // that they are kept sacred during the decoding process.
                toReturn = toReturn.replaceAll("\\+", "%2B");
                toReturn = URLDecoder.decode(toReturn, "UTF-8");
                return toReturn.replaceAll("!.*$", "");
            }
        }
        // now look in any jars we've packaged using JarFinder. Returns null when
        // no jar is found.
        return packagedClasses.get(class_file);
    }

    public static void initCredentials(Job job) throws IOException {
        UserProvider userProvider = UserProvider.instantiate(job.getConfiguration());
        if(userProvider.isHadoopSecurityEnabled() && System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
            job.getConfiguration().set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
        }

        if(userProvider.isHBaseSecurityEnabled()) {
            try {
                String ie = job.getConfiguration().get("hbase.mapred.output.quorum");
                User user = userProvider.create(UserGroupInformation.getLoginUser());
                LOG.info("Login use Name = " + user.getName() + "**********************************");
                if(ie != null) {
                    Configuration peerConf = HBaseConfiguration.create(job.getConfiguration());
                    ZKUtil.applyClusterKeyToConf(peerConf, ie);
                    obtainAuthTokenForJob(job, peerConf, user);
                }

                obtainAuthTokenForJob(job, job.getConfiguration(), user);
            } catch (InterruptedException var5) {
                LOG.info("Interrupted obtaining user authentication token");
                Thread.currentThread().interrupt();
            }
        }

    }

    private static void obtainAuthTokenForJob(Job job, Configuration conf, User user) throws IOException, InterruptedException {
        Token authToken = getAuthToken(conf, user);
        if(authToken == null) {
            user.obtainAuthTokenForJob(conf, job);
        } else {
            job.getCredentials().addToken(authToken.getService(), authToken);
        }
    }


    private static Token<AuthenticationTokenIdentifier> getAuthToken(Configuration conf, User user) throws IOException, InterruptedException {
        ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "mr-init-credentials", (Abortable)null);

        Token var4;
        try {
            String e = ZKClusterId.readClusterIdZNode(zkw);
            var4 = (new AuthenticationTokenSelector()).selectToken(new Text(e), user.getUGI().getTokens());
        } catch (KeeperException var8) {
            throw new IOException(var8);
        } finally {
            zkw.close();
        }

        return var4;
    }
}
