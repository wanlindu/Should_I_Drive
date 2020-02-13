import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class sampling_ever_10000 {
    public static void main(String[] args) throws IOException {
        BufferedReader csvReader = null;
        try {
            csvReader = new BufferedReader(new FileReader("/Users/wanlindu/Downloads/dataset/SFMTA_Parking_Meter_Detailed_Revenue_Transactions.csv "));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        int count = 0;
        String head = csvReader.readLine();
        String row = "";
        while ((row = csvReader.readLine()) != null && count < 100000) {
            String[] data = row.split(",");

            count ++;
        }
        csvReader.close();
    }

}
