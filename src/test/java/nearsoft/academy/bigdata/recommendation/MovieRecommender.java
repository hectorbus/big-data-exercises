package nearsoft.academy.bigdata.recommendation;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.BiMap;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.DataModel;

public class MovieRecommender {

  private int TotalReviews;
  private String datasetPath;
  private Map<String, Long> usersDataMap;
  private Map<Long, String> productsMap;

  public MovieRecommender(String filePath) throws IOException {
    this.TotalReviews = 0;
    this.datasetPath = "dataset.csv";
    this.usersDataMap = new HashMap<String, Long>();
    this.productsMap = HashBiMap.create();

    loadFile(filePath);
  }

  public void loadFile(String filePath) throws IOException {
    BiMap<String, Long> productsMap = HashBiMap.create();
    String currentLine;
    String userId = "";
    String productId = "";
    String score = "";

    long productsCount = 0;
    long usersCount = 0;

    InputStream fileStream = new FileInputStream(filePath);
    InputStream gzipStream = new GZIPInputStream(fileStream);
    Reader decoder = new InputStreamReader(gzipStream, "US-ASCII");
    BufferedReader br = new BufferedReader(decoder);

    Files.deleteIfExists(Paths.get(datasetPath));
    File file = new File(datasetPath);
    FileWriter fw = new FileWriter(file, true);
    BufferedWriter bw = new BufferedWriter(fw);

    while ((currentLine = br.readLine()) != null) {
      if (currentLine.startsWith("review/userId:")) {
        userId = currentLine.substring(15);
        if (!usersDataMap.containsKey(userId)) {
          usersDataMap.put(userId, usersCount);
          usersCount++;
        }
        TotalReviews++;
      }

      if (currentLine.startsWith("product/productId:")) {
        productId = currentLine.substring(19);
        if (!productsMap.containsKey(productId)) {
          productsMap.put(productId, productsCount);
          productsCount++;
        }
      }

      if (currentLine.startsWith("review/score:")) {
        score = currentLine.substring(14);
        String composedString = userId + ", " + productId + ", " + score + "\n";
        bw.write(composedString);
      }
    }

    br.close();
    bw.close();

    this.productsMap = productsMap.inverse();
  }

  public long getTotalReviews() throws IOException {
    return TotalReviews;
  }

  public long getTotalProducts() throws IOException {
    return productsMap.size();
  }

  public long getTotalUsers() throws IOException {
    return usersDataMap.size();
  }

  public List<String> getRecommendationsForUser(String userId) throws IOException, TasteException {
    DataModel model = new FileDataModel(new File(datasetPath));
    UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
    UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
    UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

    long numericUserID = usersDataMap.get(userId);

    List<RecommendedItem> recommendations = recommender.recommend(numericUserID, 3);
    List<String> recommendationIDs = new ArrayList<String>(recommendations.size());

    for (RecommendedItem recommendation : recommendations) {
      long itemId = recommendation.getItemID();
      recommendationIDs.add(this.productsMap.get(itemId));
    }

    return recommendationIDs;
  }
}