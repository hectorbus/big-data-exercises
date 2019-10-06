package nearsoft.academy.bigdata.recommendation;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.model.DataModel;

public class MovieRecommender {
  private long totalReviews;
  private long totalProducts;
  private long totalUsers;
  private String datasetFilePath;
  private HashMap<String, Long> usersMap;
  private HashMap<Long, String> inversedProductsMap;
  private HashMap<String, Long> normalProductsMap;
  private UserBasedRecommender recommender;

  public MovieRecommender(String filePath) throws IOException, TasteException {
    totalReviews = 0;
    totalProducts = 0;
    totalUsers = 0;
    datasetFilePath = "dataset.csv";
    usersMap = new HashMap<String, Long>();
    inversedProductsMap = new HashMap<Long, String>();
    normalProductsMap = new HashMap<String, Long>();

    loadFile(filePath);

    DataModel model = new FileDataModel(new File(datasetFilePath));
    UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
    UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
    recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
  }

  public List<String> getRecommendationsForUser(String userIdString) throws IOException, TasteException {
    long UserId = usersMap.get(userIdString);
    List<String> recommendationIds = new ArrayList<String>();
    List<RecommendedItem> recommendations = recommender.recommend(UserId, 3);

    for (RecommendedItem recommendation : recommendations) {
      long itemId = recommendation.getItemID();
      recommendationIds.add(inversedProductsMap.get(itemId));
    }

    return recommendationIds;
  }

  public long getTotalReviews() throws IOException {
    return totalReviews;
  }

  public long getTotalProducts() throws IOException {
    return totalProducts;
  }

  public long getTotalUsers() throws IOException {
    return totalUsers;
  }

  private void loadFile(String filePath) throws IOException {
    long currentUserId = 0;
    long currentProductId = 0;
    String currentLine;
    String userId = null;
    String productId = null;
    String score = null;

    InputStream fileStream = new FileInputStream(filePath);
    InputStream gzipStream = new GZIPInputStream(fileStream);
    Reader decoder = new InputStreamReader(gzipStream, "US-ASCII");
    BufferedReader br = new BufferedReader(decoder);

    Files.deleteIfExists(Paths.get(datasetFilePath));
    File file = new File(datasetFilePath);
    FileWriter fw = new FileWriter(file, true);
    BufferedWriter bw = new BufferedWriter(fw);

    while ((currentLine = br.readLine()) != null) {
      if (currentLine.startsWith("review/userId:")) {
        userId = currentLine.substring(15);
        if (!usersMap.containsKey(userId)) {
          usersMap.put(userId, totalUsers);
          totalUsers++;
        }
        currentUserId = usersMap.get(userId);
        totalReviews++;
      }

      if (currentLine.startsWith("product/productId:")) {
        productId = currentLine.substring(19);
        if (!normalProductsMap.containsKey(productId)) {
          normalProductsMap.put(productId, totalProducts);
          inversedProductsMap.put(totalProducts, productId);
          totalProducts++;
        }
        currentProductId = normalProductsMap.get(productId);
      }

      if (currentLine.startsWith("review/score:")) {
        score = currentLine.substring(14);
        bw.write(currentUserId + "," + currentProductId + "," + score + "\n");
      }
    }

    br.close();
    bw.close();
  }
}
