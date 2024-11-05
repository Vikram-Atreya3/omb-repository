package io.openmessaging.benchmark.storage.adapter;

import com.azure.core.util.HttpClientOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Class to connect to storage account.
 */
public class StorageAdapter {

    private static final Logger log = LoggerFactory.getLogger(StorageAdapter.class);

    public static BlobContainerAsyncClient GetAsyncStorageClient(String storageAccountName, String storageContainerName) {
        // Construct the blob container endpoint from the arguments.
        String containerEndpoint = String.format("https://%s.blob.core.windows.net/%s", storageAccountName, storageContainerName);

        return  new BlobContainerClientBuilder()
                .endpoint(containerEndpoint)
                .clientOptions(new HttpClientOptions().setMaximumConnectionPoolSize(1000))
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildAsyncClient();
    }

    public static BlobContainerClient GetSyncStorageClient(String storageAccountName, String storageContainerName) {
        // Construct the blob container endpoint from the arguments.
        String containerEndpoint = String.format("https://%s.blob.core.windows.net/%s", storageAccountName, storageContainerName);

        return  new BlobContainerClientBuilder()
                .endpoint(containerEndpoint)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();
    }

    public static List<String> readBlobFromStorage(String storageAccountName, String storageContainerName, String blobName, String fileExtension){
        BlobContainerClient blobStorageClient = GetSyncStorageClient(storageAccountName,storageContainerName);
        final BlobClient blobClient = blobStorageClient.getBlobClient(blobName+fileExtension);

        List<String> blobData = new ArrayList<>();
        try{
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(blobClient.openInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    blobData.add(line);
                }
            }
        } catch (Exception e){
            log.warn("Could not fetch blob for {}-{}-{}. Exception - {}", storageAccountName, storageContainerName, blobName, e.getLocalizedMessage());
            blobData = null;
        }
        return blobData;
    }
}