# Retail Analytics with Apache Flink

## Project Description

**Sector**:
- Retail and E-commerce

**Technologies Used**:
- **Apache Flink**: Utilized for its powerful stream and batch processing capabilities, enabling complex data transformations and analytics workflows.
- **Java**: The primary programming language used to implement Flink jobs.
- **Maven**: Employed for dependency management and to automate the building of the project.
- **Docker**: Used to containerize the Flink environment, ensuring consistent execution across different setups.
- **Lombok**: Reduces boilerplate code in Java, making the codebase cleaner and easier to maintain.

**Objective/Goal**:
- To demonstrate the capabilities of Apache Flink in processing and analyzing sales data efficiently, showcasing complex data transformations and outputs in the retail domain.

## Data Source, Transformation & Output

**Database Schema**:
Below is the schema visualization for the data models used in this project:
![Schema](./Project%20Screenshots/1.%20Schema.png)

**Data Sources**:
- Sales and product data are loaded from CSV files located in the [Datasets directory](./Datasets/).

**Transformation Steps**:
[Transformation code](./src/main/java/salesanalysis/DataBatchJob.java) does the following -
- **Data Ingestion**: Data is read from CSV files using Flink's DataSet API, mapping rows to POJOs defined for order items and products.
- **Data Joining**: Datasets are joined on `productId` to combine related data from separate files, using Flink's join operation.
- **Data Aggregation**: Aggregations are performed to calculate total sales per category using map functions to transform data and reduce functions to aggregate. The results are structured using the `CategorySalesDTO` (Data Transfer Object).
- **Sorting**: The aggregated data is sorted by total sales in descending order to prioritize higher sales categories.
- **Custom Output Formats**: A custom output format is implemented to write the aggregated data back to the system in a structured CSV format. This output format leverages the `CategorySalesDTO` to ensure data consistency and clarity.

**Output**: 
- The aggregated results are written to an [output CSV file](./Output/output.csv) detailing total sales per category.

### Deploying the Flink Job
Here is a screenshot showing how the Flink job is submitted using a JAR file:
![Submitting .jar](./Project%20Screenshots/2.%20Submitting%20.jar%20to%20Flink.png)

### Flink Job Dashboard
This image provides an overview of the Flink job dashboard where job metrics and statuses can be monitored:
![Flink Job Overview](./Project%20Screenshots/3.%20Flink%20Job%20Overview.png)

## Results

This project successfully processes raw sales and product data to compute and report the total sales per category. The results are sorted by total sales, providing clear insights into the most profitable categories. The output is stored in a CSV format, making it easy to use for further analysis or reporting.

## Learnings

Through this project, key learnings were achieved in:
- Efficient data processing and transformation using Apache Flink.
- Effective orchestration of Flink jobs using Docker for a seamless development and deployment cycle.
- Advanced data manipulation techniques including custom output formatting to tailor the output to specific requirements.