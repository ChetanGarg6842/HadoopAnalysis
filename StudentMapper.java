import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Collections;
// import java.util.List;

// import org.apache.hadoop.io.DoubleWritable;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;

// public class StudentMapper extends Mapper<LongWritable, Text, Text, StudentDataAnalysisWritable> {

//   private List<String> categories = Arrays.asList("General", "OBC", "SC", "ST");
//   private List<String> religions = Arrays.asList("Hindu", "Muslim", "Christian", "Sikh", "Other");

//   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//     if (key.get() == 0) {
//       // Skip header row
//       return;
//     }

//     String[] row = value.toString().split(",");

//     // Extract relevant columns
//     String studentId = row[0];
//     double familyIncome = Double.parseDouble(row[1]);
//     String religion = row[2];
//     String familyId = row[3];
//     String mobileNo = row[4];
//     String fatherName = row[5];
//     String motherName = row[6];
//     String admissionNo = row[7];
//     String category = row[8];
//     String gender = row[9];

//     // Calculate income range
//     String incomeRange = getIncomeRange(familyIncome);

//     // Create output key-value pairs for income range, category, religion, and gender
//     List<String> keys = new ArrayList<String>();
//     keys.add(incomeRange);
//     keys.add(category);
//     keys.add(religion);
//     keys.add(gender);

//     for (String k : keys) {
//       StudentDataAnalysisWritable outputValue = new StudentDataAnalysisWritable();
//       outputValue.setStudentId(studentId);
//       outputValue.setFamilyIncome(familyIncome);
//       outputValue.setReligion(religion);
//       outputValue.setFamilyId(familyId);
//       outputValue.setMobileNo(mobileNo);
//       outputValue.setFatherName(fatherName);
//       outputValue.setMotherName(motherName);
//       outputValue.setAdmissionNo(admissionNo);
//       outputValue.setCategory(category);
//       outputValue.setGender(gender);
//       context.write(new Text(k), outputValue);
//     }
//   }

//   // Helper function to get the income range
//   private String getIncomeRange(double income) {
//     if (income < 50000) {
//       return "Less than 50,000";
//     } else if (income < 100000) {
//       return "50,000 - 99,999";
//     } else if (income < 150000) {
//       return "100,000 - 149,999";
//     } else if (income < 200000) {
//       return "150,000 - 199,999";
//     } else {
//       return "200,000 or more";
//     }
//   }

// }


public class StudentMapper extends Mapper<LongWritable, Text, Text, StudentWritable> {
  
  private Text outputKey = new Text();
  private StudentWritable outputValue = new StudentWritable();
  private Map<String, List<Double>> incomeRangeMap = new HashMap<>();
  private Map<String, List<Double>> categoryMap = new HashMap<>();
  private Map<String, List<Double>> religionMap = new HashMap<>();
  private Map<String, List<Double>> genderMap = new HashMap<>();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] fields = line.split(",");
    
    // Parse fields
    String studentId = fields[0];
    double familyIncome = Double.parseDouble(fields[1]);
    String religion = fields[2];
    String familyId = fields[3];
    String mobileNo = fields[4];
    String fatherName = fields[5];
    String motherName = fields[6];
    String admissionNo = fields[7];
    String category = fields[8];
    String gender = fields[9];
    
    // Output key-value pairs
    outputKey.set(studentId);
    outputValue.setFamilyIncome(new DoubleWritable(familyIncome));
    outputValue.setReligion(new Text(religion));
    outputValue.setFamilyId(new Text(familyId));
    outputValue.setMobileNo(new Text(mobileNo));
    outputValue.setFatherName(new Text(fatherName));
    outputValue.setMotherName(new Text(motherName));
    outputValue.setAdmissionNo(new Text(admissionNo));
    outputValue.setCategory(new Text(category));
    outputValue.setGender(new Text(gender));
    context.write(outputKey, outputValue);
    
    // Update income range map
    if (incomeRangeMap.containsKey(familyId)) {
      incomeRangeMap.get(familyId).add(familyIncome);
    } else {
      List<Double> incomeList = new ArrayList<>();
      incomeList.add(familyIncome);
      incomeRangeMap.put(familyId, incomeList);
    }
    
    // Update category map
    if (categoryMap.containsKey(category)) {
      categoryMap.get(category).add(familyIncome);
    } else {
      List<Double> incomeList = new ArrayList<>();
      incomeList.add(familyIncome);
      categoryMap.put(category, incomeList);
    }
    
    // Update religion map
    if (religionMap.containsKey(religion)) {
      religionMap.get(religion).add(familyIncome);
    } else {
      List<Double> incomeList = new ArrayList<>();
      incomeList.add(familyIncome);
      religionMap.put(religion, incomeList);
    }
    
    // Update gender map
    if (genderMap.containsKey(gender)) {
      genderMap.get(gender).add(familyIncome);
    } else {
      List<Double> incomeList = new ArrayList<>();
      incomeList.add(familyIncome);
      genderMap.put(gender, incomeList);
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    // Calculate mean, median, and mode for each map
    for (Map.Entry<String, List<Double>> entry : incomeRangeMap.entrySet()) {
      String range = entry.getKey();
      List<Double> incomeList =entry.getValue();

  // Mean
  double sum = 0.0;
  for (double income : incomeList) {
    sum += income;
  }
  double mean = sum / incomeList.size();
  
  // Median
  Collections.sort(incomeList);
  int middle = incomeList.size() / 2;
  double median;
  if (incomeList.size() % 2 == 0) {
    median = (incomeList.get(middle - 1) + incomeList.get(middle)) / 2;
  } else {
    median = incomeList.get(middle);
  }
  
  // Mode
  Map<Double, Integer> frequencyMap = new HashMap<>();
  for (double income : incomeList) {
    if (frequencyMap.containsKey(income)) {
      frequencyMap.put(income, frequencyMap.get(income) + 1);
    } else {
      frequencyMap.put(income, 1);
    }
  }
  double mode = 0.0;
  int maxFrequency = 0;
  for (Map.Entry<Double, Integer> freqEntry : frequencyMap.entrySet()) {
    if (freqEntry.getValue() > maxFrequency) {
      mode = freqEntry.getKey();
      maxFrequency = freqEntry.getValue();
    }
  }
  
  // Output key-value pairs
  outputKey.set("Income Range: " + range);
  outputValue.setMean(new DoubleWritable(mean));
  outputValue.setMedian(new DoubleWritable(median));
  outputValue.setMode(new DoubleWritable(mode));
  context.write(outputKey, outputValue);
}

// Repeat the above process for categoryMap, religionMap, and genderMap
// ... (code omitted for brevity)
 }}

// Calculate mean, median, and mode for each map
for (Map.Entry<String, List<Double>> entry : categoryMap.entrySet()) {
  String category = entry.getKey();
  List<Double> incomeList = entry.getValue();
  
  // Calculate mean
  double mean = 0;
  for (double income : incomeList) {
    mean += income;
  }
  mean /= incomeList.size();
  
  // Calculate median
  Collections.sort(incomeList);
  double median = 0;
  int middle = incomeList.size() / 2;
  if (incomeList.size() % 2 == 0) {
    median = (incomeList.get(middle - 1) + incomeList.get(middle)) / 2;
  } else {
    median = incomeList.get(middle);
  }
  
  // Calculate mode
  Map<Double, Integer> frequencyMap = new HashMap<>();
  for (double income : incomeList) {
    if (frequencyMap.containsKey(income)) {
      frequencyMap.put(income, frequencyMap.get(income) + 1);
    } else {
      frequencyMap.put(income, 1);
    }
  }
  double mode = 0;
  int maxFrequency = 0;
  for (Map.Entry<Double, Integer> freqEntry : frequencyMap.entrySet()) {
    if (freqEntry.getValue() > maxFrequency) {
      mode = freqEntry.getKey();
      maxFrequency = freqEntry.getValue();
    }
  }
  
  // Output key-value pairs
  outputKey.set(category);
  outputValue.setMean(new DoubleWritable(mean));
  outputValue.setMedian(new DoubleWritable(median));
  outputValue.setMode(new DoubleWritable(mode));
  context.write(outputKey, outputValue);
}

// Update religion map
for (Map.Entry<String, List<Double>> entry : religionMap.entrySet()) {
      String religion = entry.getKey();
      List<Double> incomeList = entry.getValue();
      Collections.sort(incomeList);

      // Calculate mean
      double sum = 0;
      for (double income : incomeList) {
        sum += income;
      }
      double mean = sum / incomeList.size();
      
      // Calculate median
      double median;
      if (incomeList.size() %== 0) {
median = (incomeList.get(incomeList.size() / 2) + incomeList.get(incomeList.size() / 2 - 1)) / 2;
} else {
median = incomeList.get(incomeList.size() / 2);
}
  // Calculate mode
  Map<Double, Integer> incomeCount = new HashMap<>();
  int maxCount = 0;
  double mode = 0;
  for (double income : incomeList) {
    int count = incomeCount.getOrDefault(income, 0) + 1;
    incomeCount.put(income, count);
    if (count > maxCount) {
      maxCount = count;
      mode = income;
    }
  }

  // Emit mean, median, and mode for religion
  context.write(new Text(religion), new ReligionWritable(new DoubleWritable(mean), new DoubleWritable(median), new DoubleWritable(mode)));
}

// Calculate mean, median, and mode for genderMap
for (Map.Entry<String, List<Double>> entry : genderMap.entrySet()) {
  String gender = entry.getKey();
  List<Double> incomeList = entry.getValue();
  int count = incomeList.size();

  // Calculate mean
  double sum = 0;
  for (double income : incomeList) {
    sum += income;
  }
  double mean = sum / count;

  // Calculate median
  Collections.sort(incomeList);
  double median;
  if (count % 2 == 0) {
    median = (incomeList.get(count / 2 - 1) + incomeList.get(count / 2)) / 2;
  } else {
    median = incomeList.get(count / 2);
  }

  // Calculate mode
  Map<Double, Integer> frequencyMap = new HashMap<>();
  int maxFrequency = 0;
  double mode = 0;
  for (double income : incomeList) {
    int frequency = frequencyMap.getOrDefault(income, 0) + 1;
    frequencyMap.put(income, frequency);
    if (frequency > maxFrequency) {
      maxFrequency = frequency;
      mode = income;
    }
  }

  // Create output value
  StudentStatsWritable outputValue = new StudentStatsWritable();
  outputValue.setCount(new IntWritable(count));
  outputValue.setMean(new DoubleWritable(mean));
  outputValue.setMedian(new DoubleWritable(median));
  outputValue.setMode(new DoubleWritable(mode));

  // Write output key-value pair
  outputKey.set("gender_" + gender);
  context.write(outputKey, outputValue);




// jfree code is wriiten and in between all mean , median mode code is wriiten
// this code is already written just need to write bar chart lines

//@Override
//public void cleanup(Context context) throws IOException, InterruptedException {
  // Calculate mean, median, and mode for each map
 // for (Map.Entry<String, List<Double>> entry : incomeRangeMap.entrySet()) {
  //  String range = entry.getKey();
  //  List<Double> incomeList = entry.getValue();
  //  double mean = calculateMean(incomeList);
  // double median = calculateMedian(incomeList);
  //  double mode = calculateMode(incomeList);
    // Emit mean, median, and mode for each income range
  //  ...
 // }

  // Generate bar charts for each map
  BarChart.generateCategoryIncomeChart(categoryMap);
  BarChart.generateReligionIncomeChart(religionMap);
  BarChart.generateGenderIncomeChart(genderMap);

}