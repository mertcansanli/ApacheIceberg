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
        """
        Bronze layer seed task'ı
        Verileri kaynak sistemlerden bronze tablolarına yükler
        """
        import logging
        from operators.dbt_operator import DbtOperator
        logger = logging.getLogger(__name__)
        logger.info("Seeding bronze: ")  # Bronze seeding başlangıç logu
        
        try:
            # Trino bağlantısı kontrolü - bronze tabloları dolu mu?
            import sqlalchemy
            from sqlalchemy import text
            
            # Trino bağlantısı (Iceberg catalog üzerinden bronze schema'ya)
            engine = sqlalchemy.create_engine('trino://trino@trino-coordinator:8080/iceberg/bronze')
            with engine.connect() as conn:
                # raw_customer_events tablosundaki kayıt sayısını kontrol et
                result = conn.execute(text("SELECT count(*) as cnt FROM raw_customer_events"))
                row_count = result.scalar()
                
                if row_count and row_count > 0:
                    logger.info(f"Bronze is already seeded with {row_count} rows")  # Zaten dolu
                    return {
                        'status': 'skipped',  # Seeding atlanıyor
                        'layer': 'bronze_seed',
                        'row_count': row_count,
                        'message': 'tables are already seeded'
                    }
        except Exception as e:
            # Tablo yoksa veya bağlantı hatası varsa devam et (normal durum)
            logger.error(f"Error checking bronze seeding: {e}")
        
        # DBT operator ile seeding işlemi
        operator = DbtOperator(
            task_id='seed_bronze_data_internal',
            dbt_root_dir=DBT_ROOT_DIR,
            dbt_command='seed',  # DBT seed komutu
            full_refresh=True,  # Tabloları tamamen yenile
        )
        
        try:
            operator.execute(context={})  # DBT seeding'i çalıştır
            return {
                'status': 'success',
                'layer': 'bronze_seed',
                'pipeline_id': pipeline_metadata['pipeline_id'],
                'timestamp': datetime.now().isoformat(),
            }
        except Exception as e:  # Hata yakalama düzeltildi
            logger.warning(f'Error! Something went wrong: {e}')
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
    
    # Task'ları birbirine bağla (dependency chain)
    # start_pipeline -> seed_bronze -> transform_bronze_layer
    metadata = start_pipeline()
    seed_result = seed_bronze(metadata)
    transform_bronze_layer(seed_result)

# DAG'ı oluştur (decorator ile tanımlandığı için çağrılması gerekir)
dag_pipeline_instance = dag_pipeline()