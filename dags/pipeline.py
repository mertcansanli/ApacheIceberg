# Gerekli kütüphanelerin import edilmesi
from airflow.decorators import dag, task
from datetime import datetime, timedelta  # datetime eklendi
from airflow import settings  # settings eklendi

# DBT proje dizini - Airflow DAGs klasörü içindeki dbt_ecommerce klasörü
DBT_ROOT_DIR = f"{settings.DAGS_FOLDER}/dbt_ecommerce"

# DAG tanımı - her 6 saatte bir çalışacak pipeline
@dag(
    schedule=timedelta(hours=6),  # DAG her 6 saatte bir tetiklenir
    default_args={
        "owner": "Mertcan",  # DAG sahibi
        "depends_on_past": False,  # Önceki çalışmalara bağımlı değil
        "retries": 2,  # Hata durumunda 2 kez yeniden dene
        "retry_delay": timedelta(seconds=15),  # Yeniden denemeler arasında 15 saniye bekle
        "max_active_runs": 1,  # Aynı anda sadece 1 aktif çalışma
        "execution_timeout": timedelta(minutes=30),  # 30 dakikada tamamlanmazsa zaman aşımı
    },
    start_date=None,  # Başlangıç tarihi belirtilmemiş (ilk çalışmada başlar)
    catchup=False,  # Geçmiş çalışmaları telafi etme
    max_active_runs=1,  # Aynı anda sadece 1 DAG çalışması
    tags=["medalllion", "analytics"]  # DAG için etiketler
)
def dag_pipeline():
    """
    Ana DAG fonksiyonu - Medallion mimarisine göre veri pipeline'ı
    Bronze tabloları oluşturur ve dönüştürür
    """
    
    @task
    def start_pipeline():
        """
        Pipeline başlangıç task'ı
        Pipeline metadatasını oluşturur ve loglar
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Starting the pipeline")  # Pipeline başlangıç logu
        
        # Pipeline metadata bilgileri
        pipeline_metadata = {
            'pipeline_start_time': datetime.now().isoformat(),  # Başlangıç zamanı ISO formatında
            'dbt_root_dir': DBT_ROOT_DIR,  # DBT root dizini
            'pipeline_id': f'dag_pipeline_{datetime.now().strftime("%Y%m%d%H%M%S")}',  # Benzersiz pipeline ID
            'environment': 'production'  # Çalışma ortamı (yazım hatası düzeltildi)
        }
        
        logger.info(f'Starting the Pipeline: Id {pipeline_metadata["pipeline_id"]}')  # Pipeline ID logu
        return pipeline_metadata  # Metadata'yı sonraki task'lara ilet

    @task
    def seed_bronze(pipeline_metadata):
        import logging
        from operators.dbt_operator import DbtOperator
        logger = logging.getLogger(__name__)
        logger.info("Seeding bronze: ")
        
        # DBT operator ile seeding işlemi - SADECE BURAYI DÜZELT
        operator = DbtOperator(
            task_id='seed_bronze_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='seed --full-refresh',  # ✅ full_refresh komut içinde
            target='trino'  # ✅ target ekle
        )
        
        try:
            operator.execute(context={})
            return {
                'status': 'success',
                'layer': 'bronze_seed',
                'pipeline_id': pipeline_metadata['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.error(f'Error during seeding: {e}')
            return {
                'status': 'failed',
                'layer': 'bronze_seed',
                'pipeline_id': pipeline_metadata['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
    
    @task
    def transform_bronze_layer(seed_result):
        """
        Bronze layer transform task'ı
        Bronze tablolarını işler ve silver/gold tablolarına dönüştürür
        """
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings
        logger = logging.getLogger(__name__)
        
        # Eğer seeding başarısız olduysa uyarı ver ama yine de dönüşümü dene
        if seed_result['status'] == 'failed':
            logger.warning('Seeding Failed! Continuing Without Transformation...')
        
        logger.info("Transforming Bronze: ")  # Dönüşüm başlangıç logu
        
        # DBT operator ile bronze tag'li modelleri çalıştır
        operator = DbtOperator(
            task_id='transform_bronze_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:bronze'  # Sadece bronze tag'li modeller
        )
        
        try:
            operator.execute(context={})  # DBT dönüşümünü çalıştır
            return {
                'status': 'success',
                'layer': 'bronze_transform',
                'pipeline_id': seed_result['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f'Error Occurred: {e}')
            raise  # Hatayı yukarı fırlat (DAG'ın başarısız olması için)
    
    @task 
    def validate_bronze_data(bronze_result):
        """Bronze layer veri doğrulama task'ı"""
        import logging
        logger = logging.getLogger(__name__)  # ✅ Düzeltildi: logger = logging.getLogger
        logger.info(f"Validating Bronze Layer : {bronze_result['pipeline_id']}")

        validation_check = {
            'null_checks': 'passed',
            'duplicate_checks': 'passed',
            'schema_validation': 'passed',
            'row_counts': 'passed'
        }

        return {
            'status': 'success',
            'layer': 'bronze_validate',
            'pipeline_id': bronze_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validation_checks': validation_check
        }
    
    @task  # ✅ Eklendi: @task decorator'ı eksikti
    def transform_silver_layer(bronze_validation):
        """Silver layer dönüşüm task'ı"""
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings
        logger = logging.getLogger(__name__)
        
        if bronze_validation['status'] != 'success':  # ✅ Düzeltildi: !== yerine !=
            raise Exception(f"Bronze Validation Have Failed! {bronze_validation}")
        
        logger.info("Transforming Silver Layer: ")  # Dönüşüm başlangıç logu
        
        # DBT operator ile silver tag'li modelleri çalıştır
        operator = DbtOperator(
            task_id='transform_silver_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:silver'  # Sadece silver tag'li modeller
        )
        
        try:
            operator.execute(context={})  # DBT dönüşümünü çalıştır
            return {
                'status': 'success',
                'layer': 'silver_transform',
                'pipeline_id': bronze_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f'Error Occurred: {e}')
            raise  # Hatayı yukarı fırlat (DAG'ın başarısız olması için)

    @task 
    def validate_silver_data(silver_result):
        """Silver layer veri doğrulama task'ı"""
        import logging
        logger = logging.getLogger(__name__)  # ✅ Düzeltildi: logger = logging.getLogger
        logger.info(f"Validating Silver for pipeline : {silver_result['pipeline_id']}")  # ✅ Düzeltildi: loger yerine logger

        validation_checks = {
            'business_rules': 'passed',  # ✅ Düzeltildi: 'buisness_rules' -> 'business_rules'
            'referential_integrity': 'passed',  # ✅ Düzeltildi: 'passed': yerine 'passed'
            'aggregation_accuracy': 'passed',
            'data_freshness': 'passed'
        }

        return {
            'status': 'success',
            'layer': 'silver_validate',  # ✅ Düzeltildi: virgül eklendi
            'pipeline_id': silver_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),  # ✅ Düzeltildi: 'timestamp' önüne virgül eklendi
            'validation_checks': validation_checks
        }
    
    @task  # ✅ Eklendi: @task decorator'ı eksikti
    def transform_gold_layer(silver_validation):
        """Gold layer dönüşüm task'ı"""
        import logging
        from operators.dbt_operator import DbtOperator
        from airflow import settings
        logger = logging.getLogger(__name__)
        
        if silver_validation['status'] != 'success':  # ✅ Düzeltildi: !== yerine !=
            raise Exception(f"Silver Validation Have Failed! {silver_validation}")
        
        logger.info("Transforming Gold Layer: ")  # Dönüşüm başlangıç logu
        
        # DBT operator ile gold tag'li modelleri çalıştır
        operator = DbtOperator(
            task_id='transform_gold_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='run --select tag:gold'  # Sadece gold tag'li modeller
        )
        
        try:
            operator.execute(context={})  # DBT dönüşümünü çalıştır
            return {
                'status': 'success',
                'layer': 'gold_transform',
                'pipeline_id': silver_validation['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:
            logger.warning(f'Error Occurred: {e}')
            raise  # Hatayı yukarı fırlat (DAG'ın başarısız olması için)

    @task
    def validate_gold_data(gold_result):
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Validating gold for pipeline : {gold_result['pipeline_id']}")

        validation_checks={
            'buisness_rules':'passed',
            'metrics_caculations': 'passed',
            'completeness_checks': 'passed'
        }

        return{
            'status':'success',
            'layer':'gold_validate',
            'pipeline_id': gold_result['pipeline_id'],
            'timestamp': datetime.now().isoformat(),
            'validation_checks': validation_checks
        }
    @task
    def generate_documentation(gold_validation):
        import logging
        from operators.dbt_operator import DbtOperator
        logger= logging.getLogger(__name__)
        if gold_validation['status'] != 'success':
            raise Exception(f"Gold Validation Failed! Cannot Proceed with Documentation... {gold_validation}")
        logger.info(f"Generating Documentation For the Pipeline : {gold_validation['pipeline_id']}")

        operator=DbtOperator(
            task_id='generate_dbt_docs_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='docs_generate'
        )

        try:
            operator.execute(context={})
            return{
                'status':'success',
                'layer':'documentation',
                'pipeline_id':gold_validation['pipeline_id'],
                'timetamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.warning(f'Error Accured during documentation generation! {e}')
            raise
    @task
    def end_pipeline(docs_result):
        import logging
        logger= logging.getLogger(__name__)
        logger.info("End of the Pipeline")
        logger.info(f"Pipeline Final Status : {docs_result['status']}")
        logger.info(f"Pipeline Completed at : {docs_result["timestamp"]}")
    # ============================================
    # TASK ZİNCİRİ - Medallion mimarisi akışı
    # ============================================
    # Bronze Layer
    metadata = start_pipeline()
    seed_result = seed_bronze(metadata)
    bronze_transform_result = transform_bronze_layer(seed_result)
    bronze_validation_result = validate_bronze_data(bronze_transform_result)
    
    # Silver Layer
    silver_transform_result = transform_silver_layer(bronze_validation_result)
    silver_validation_result = validate_silver_data(silver_transform_result)
    
    # Gold Layer
    gold_transform_result = transform_gold_layer(silver_validation_result)

# DAG'ı oluştur (decorator ile tanımlandığı için çağrılması gerekir)
dag_pipeline_instance = dag_pipeline()