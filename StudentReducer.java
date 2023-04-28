import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StudentDataAnalysisReducer extends Reducer<Text, StudentDataWritable, Text, Text> {
    
    // Constants for income ranges
    private static final double INCOME_RANGE_1 = 10000.0;
    private static final double INCOME_RANGE_2 = 20000.0;
    private static final double INCOME_RANGE_3 = 30000.0;
    private static final double INCOME_RANGE_4 = 40000.0;
    private static final double INCOME_RANGE_5 = 50000.0;
    
    // Maps to store data for mean, median, and mode calculations
    private Map<String, List<Double>> incomeRangeMap = new HashMap<>();
    private Map<String, List<Double>> categoryMap = new HashMap<>();
    private Map<String, List<Double>> religionMap = new HashMap<>();
    private Map<String, List<Double>> genderMap = new HashMap<>();
    
    // List to store family income for comparison
    private List<Double> familyIncomeList = new ArrayList<>();
    
    @Override
    public void reduce(Text key, Iterable<StudentDataWritable> values, Context context) throws IOException, InterruptedException {
        // Initialize variables for mean, median, and mode calculations
        double sum = 0;
        List<Double> incomeList = new ArrayList<>();
        Map<Double, Integer> incomeCount = new HashMap<>();
        int maxCount = 0;
        double mode = 0;
        
        // Process each student record
        for (StudentDataWritable value : values) {
            // Add family income to list for comparison
            familyIncomeList.add(value.getFamilyIncome().get());
            
            // Add income to list for mean, median, and mode calculations
            incomeList.add(value.getFamilyIncome().get());
            sum += value.getFamilyIncome().get();
            
            // Increment count for mode calculation
            double income = value.getFamilyIncome().get();
            int count = incomeCount.getOrDefault(income, 0) + 1;
            incomeCount.put(income, count);
            if (count > maxCount) {
                maxCount = count;
                mode = income;
            }
        }
        
        // Calculate mean, median, and mode
        double mean = sum / incomeList.size();
        Collections.sort(incomeList);
        double median = incomeList.get(incomeList.size() / 2);
        
        // Add income to map for group calculations
        if (key.toString().startsWith("IncomeRange")) {
            incomeRangeMap.put(key.toString(), incomeList);
        } else if (key.toString().startsWith("Category")) {
            categoryMap.put(key.toString(), incomeList);
        } else if (key.toString().startsWith("Religion")) {
            religionMap.put(key.toString(), incomeList);
        } else if (key.toString().startsWith("Gender")) {
            genderMap.put(key.toString(), incomeList);
        }
        
        // Emit results
        context.write(key, new Text("Mean: " + mean + ", Median: " + median + ", Mode: " + mode));
    }
    
    @Override
   
