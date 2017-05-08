package org.edx.hadoop.input;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;


public class ManifestTextInputFormat extends KeyValueTextInputFormat {
    protected FileStatus[] listStatus(JobConf job) throws IOException {
        FileStatus[] manifests = super.listStatus(job);
        List<FileStatus> paths = new ArrayList<FileStatus>();
        for(int i = 0; i < manifests.length; i++) {
            List<Path> globPaths = this.readManifest(manifests[i].getPath(), job);
            for (Path globPath : globPaths) {
                if (globPath != null) {
                    FileStatus fs = new FileStatus();
                    fs.setPath(globPath);
                    paths.add(fs);
                }
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
}
