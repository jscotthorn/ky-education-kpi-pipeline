# Kentucky Education Data Portal

## Data Repository Notice

**⚠️ Data files are not included in this Git repository due to file size constraints.**

All education data files (both raw and processed) are available through the Kentucky Education Data Portal:

## 🌐 **Access Data Online**

**Portal Homepage**: [https://education.kyopengov.org/data/](https://education.kyopengov.org/data/)

### Available Data Sets

| Data Type | Description | Portal Link |
|-----------|-------------|-------------|
| **🎯 Master KPI** | Consolidated dataset (175.1 MB) | [KPI Data](https://education.kyopengov.org/data/kpi/) |
| **⚙️ Processed Files** | Cleaned, standardized metrics | [Processed Data](https://education.kyopengov.org/data/processed/) |
| **📁 Raw Data** | Original KY Dept of Education files | [Raw Data](https://education.kyopengov.org/data/raw/) |

### Education Metrics Available

- **Chronic Absenteeism** - Student attendance patterns
- **English Learner Progress** - Language proficiency development  
- **Graduation Rates** - 4-year high school outcomes
- **Kindergarten Readiness** - School readiness screening
- **Out of School Suspension** - Student discipline data
- **Postsecondary Enrollment** - College enrollment tracking
- **Postsecondary Readiness** - College/career readiness indicators

## 📊 Data Format Standards

All processed data follows the **standardized 10-column KPI format**:
- `district`, `school_id`, `school_name`, `year`, `student_group`
- `metric`, `value`, `suppressed`, `source_file`, `last_updated`

## 🏛️ About the Portal

The Kentucky Education Data Portal is hosted by the [Kentucky Open Government Coalition](https://kyopengov.org) and provides transparent access to education performance data for Kentucky schools and districts.

- **Data Source**: Kentucky Department of Education School Report Card Datasets
- **Processing**: Demographic standardization with audit trails
- **Format**: Long-format KPI data with suppression transparency
- **Updates**: Regular refresh with new KY DOE releases

## 🔧 Local Development

To work with data locally:

1. **Download from Portal**: Use the web interface to download specific datasets
2. **Place in Subdirectories**: 
   - Raw files → `data/raw/{metric}/`
   - Processed files → `data/processed/`
3. **Run ETL Pipeline**: `python3 etl_runner.py` (when data is present)

## 📈 Data Processing Pipeline

The ETL modules in this repository process raw data files to create the standardized outputs available on the portal:

```
Raw Data (CSV) → ETL Processing → KPI Format → Portal Upload
```

Each metric has its own ETL module and corresponding test suite to ensure data quality and consistency.

---

**Questions?** Visit the [project repository](https://github.com/jscotthorn/ky-education-kpi-pipeline) or access data directly through the portal.