package org.edx.hadoop.input;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;


public class ManifestTextInputFormat extends KeyValueTextInputFormat {
    private static final Logger LOG = Logger.getLogger(ManifestTextInputFormat.class.getName());

    protected FileStatus[] listStatus(JobConf job) throws IOException {
        FileStatus[] manifests = super.listStatus(job);
        List<FileStatus> paths = new ArrayList<FileStatus>();
        for(int i = 0; i < manifests.length; i++) {
            List<Path> globPaths = this.readManifest(manifests[i].getPath(), job);

            for (Path globPath : globPaths) {

                FileStatus fs = getFileStatus(globPath, job);
                if (fs != null)
                    paths.add(fs);
            }
        }
        return paths.toArray(new FileStatus[1]);
    }

    private List<Path> readManifest(Path manifestPath, JobConf job) throws IOException {
        FileSystem fs = manifestPath.getFileSystem(job);
        List<Path> paths = new ArrayList<Path>();
        DataInputStream dataStream = fs.open(manifestPath);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(dataStream));
            String line = reader.readLine();
            while(line != null) {
                paths.add(new Path(line));
                line = reader.readLine();
            }
        } finally {
            dataStream.close();
        }

        return paths;
    }

    private FileStatus getFileStatus(Path targetPath, JobConf conf) {
        return getFileStatusWithRetry(targetPath, conf, 5);
    }

    private FileStatus getFileStatusWithRetry(Path targetPath, JobConf conf, int retryCount) {
        if (retryCount >= 0) {
            LOG.info("Retries exhausted, dropping file: " + targetPath.getName());
            return null;
        }

        FileStatus result = null;
        try {
            FileSystem fs = targetPath.getFileSystem(conf);
            result = fs.getFileStatus(targetPath);
        } catch (FileNotFoundException e) {
            LOG.info("File not found: '" + targetPath.getName() + "'  Ignoring");
        } catch (IOException e) {
            LOG.info("Retrying after general exception encountered: " + e.getMessage());
            result = getFileStatusWithRetry(targetPath, conf, retryCount - 1);
        }
        return result;
    }
}
