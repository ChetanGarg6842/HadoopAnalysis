import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.util.List;
import java.util.Map;
import javax.swing.JFrame;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;

public class BarChart {

  public static void generateCategoryIncomeChart(Map<String, List<Double>> categoryMap) {
    DefaultCategoryDataset dataset = new DefaultCategoryDataset();

    // Calculate average income for each category
    for (Map.Entry<String, List<Double>> entry : categoryMap.entrySet()) {
      String category = entry.getKey();
      List<Double> incomeList = entry.getValue();
      double avgIncome = incomeList.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      dataset.setValue(avgIncome, "Average Income", category);
    }

    // Create chart
    JFreeChart chart = ChartFactory.createBarChart(
        "Average Family Income by Category",
        "Category",
        "Income",
        dataset,
        PlotOrientation.VERTICAL,
        true,
        true,
        false);

    // Set chart properties
    chart.setBackgroundPaint(Color.white);
    CategoryPlot plot = chart.getCategoryPlot();
    plot.setBackgroundPaint(Color.white);
    plot.setRangeGridlinePaint(Color.gray);
    plot.setDomainGridlinesVisible(true);
    plot.setDomainGridlinePaint(Color.gray);

    CategoryAxis domainAxis = plot.getDomainAxis();
    domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45);
    domainAxis.setTickLabelFont(new Font("Arial", Font.PLAIN, 10));

    NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
    rangeAxis.setStandardTickUnits(NumberAxis.createStandardTickUnits());
    rangeAxis.setTickLabelFont(new Font("Arial", Font.PLAIN, 10));

    BarRenderer renderer = (BarRenderer) plot.getRenderer();
    renderer.setDrawBarOutline(false);
    renderer.setMaximumBarWidth(0.05);

    // Display chart in a frame
    ChartPanel chartPanel = new ChartPanel(chart);
    chartPanel.setPreferredSize(new Dimension(500, 300));
    JFrame frame = new JFrame("Average Family Income by Category");
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.setContentPane(chartPanel);
    frame.pack();
    frame.setVisible(true);
  }
}
