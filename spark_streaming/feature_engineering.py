from pyspark.sql.functions import (
    col, when, length, lower, regexp_replace, lit, coalesce
)
from pyspark.ml.feature import VectorAssembler

def add_engineered_features(df):
    """
    Add engineered features using only PySpark SQL functions (NO UDFs).X
    Uses actual database column names and handles BOOLEAN types correctly
    """
    # ✅ Step 1: Map to simpler names for processing
    df = df.withColumn('title', coalesce(col('job_title'), lit('')))
    df = df.withColumn('description', coalesce(col('job_description'), lit('')))
    
    # ✅ Step 2: Fill nulls for text fields using actual column names
    text_columns = {
        'requirements': '',
        'benefits': '',
        'salary_range': ''
    }
    
    # Fill only columns that exist
    for col_name, default_val in text_columns.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, coalesce(col(col_name), lit(default_val)))
    
    # ✅ Step 3: Combined text for keyword checks
    df = df.withColumn('combined_text', lower(col('description')))
    
    # 1. Urgent keywords count
    df = df.withColumn('urgent_count',
        when(col('combined_text').rlike('(?i)urgent|immediate|hurry|asap|act now'), 1).otherwise(0))
    
    # 2. Fee keywords count  
    df = df.withColumn('fee_count',
        when(col('combined_text').rlike('(?i)fee|deposit|payment|registration'), 1).otherwise(0))
    
    # 3. Contact red flags
    df = df.withColumn('contact_red_flag',
        when(col('combined_text').rlike('(?i)whatsapp|telegram|gmail|yahoo|outlook'), 1).otherwise(0))
    
    # 4. Salary red flags
    df = df.withColumn('salary_red_flag',
        when((col('salary_range').isNull()) | (length(col('salary_range')) == 0), 1).otherwise(0))
    
    # 5. Experience red flags
    df = df.withColumn('experience_red_flag',
        when(col('combined_text').rlike('(?i)no experience|anyone can apply|easy job'), 1).otherwise(0))
    
    # 6. Exclamation count
    df = df.withColumn('exclamation',
        length(col('combined_text')) - length(regexp_replace(col('combined_text'), '!', '')))
    
    # 7. Capital ratio
    df = df.withColumn('capital_ratio',
        when(length(col('title')) > 0,
            (length(regexp_replace(col('title'), '[^A-Z]', '')) * 100.0) / length(col('title')))
        .otherwise(0))
    
    # 8. Text lengths
    df = df.withColumn('title_length', length(col('title')))
    df = df.withColumn('desc_length', length(col('description')))
    
    # 9. Total red flags
    df = df.withColumn('total_red_flags',
        col('urgent_count') + col('fee_count') + col('contact_red_flag') + 
        col('experience_red_flag') + col('salary_red_flag'))
    
    # 10. Email presence
    df = df.withColumn('has_email',
        when(col('combined_text').rlike('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}'), 1)
        .otherwise(0))
    
    # 11. Phone presence
    df = df.withColumn('has_phone',
        when(col('combined_text').rlike('\\d{3}[-.\\s]?\\d{3}[-.\\s]?\\d{4}'), 1)
        .otherwise(0))
    
    # 12. Salary presence feature (renamed to avoid conflict)
    df = df.withColumn('has_salary_feature',
        when((col('salary_range').isNotNull()) & (length(col('salary_range')) > 0), 1)
        .otherwise(0))
    
    # ✅ 13. Logo presence - FIXED: Handle BOOLEAN type correctly
    if 'has_company_logo' in df.columns:
        df = df.withColumn('has_logo',
            when(col('has_company_logo') == True, 1)  # Compare with True, not 1
            .otherwise(0))
    else:
        df = df.withColumn('has_logo', lit(0))
    
    # ✅ 14. Telecommuting feature - FIXED: Handle BOOLEAN type correctly
    if 'telecommuting' in df.columns:
        df = df.withColumn('telecommuting_feature',
            when(col('telecommuting') == True, 1)  # Compare with True, not 1
            .otherwise(0))
    else:
        df = df.withColumn('telecommuting_feature', lit(0))
    
    # ✅ 15. Questions feature - FIXED: Handle BOOLEAN type correctly
    if 'has_questions' in df.columns:
        df = df.withColumn('has_questions_feature',
            when(col('has_questions') == True, 1)  # Compare with True, not 1
            .otherwise(0))
    else:
        df = df.withColumn('has_questions_feature', lit(0))
    
    return df


def prepare_ml_features(df):
    """
    Prepare final features for ML model
    """
    df = add_engineered_features(df)
    
    feature_cols = [
        'urgent_count', 'fee_count', 'contact_red_flag', 'salary_red_flag',
        'experience_red_flag', 'exclamation', 'capital_ratio', 'title_length',
        'desc_length', 'total_red_flags', 'has_email', 'has_phone', 
        'has_salary_feature', 'has_logo', 'telecommuting_feature', 'has_questions_feature'
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    df = assembler.transform(df)
    
    # ✅ Handle fraudulent column - FIXED: it's BOOLEAN in your database
    df = df.withColumn('label',
        when(col('fraudulent') == True, 1)  # Compare with True, not 1
        .otherwise(0))
    
    return df.select('features', 'label')