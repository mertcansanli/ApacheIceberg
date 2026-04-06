from airflow.sdk import BaseOperator #airflow temel operatör sınıfı
from airflow.exceptions import AirflowException #hata yönetimi
from dbt.cli.main import dbtRunner, dbtRunnerResult #dbt'nin python apisi
import os 
from pathlib import Path
from airflow.utils.context import Context  # Context tipi için
class DbtOperator(BaseOperator): #airflowun base operatörünü miras alır
    def __init__ (
            self,
            dbt_root_dir:str, #projenin kök dizini
            dbt_command: str, # çalıştırılacak dbt komutu
            target: str = None, #hangi profili kullanacağı
        
            dbt_vars: dict = None, # dbt değişkenleri
            select: str = None, #hangi modelleri seçeceği
            full_refresh: bool = False, # tabloları yeniden oluşturma 
            **kwargs #base operator parametreleri
    ):
        super().__init__(**kwargs)
        self.dbt_root_dir = dbt_root_dir
        self.dbt_command = dbt_command
        self.target = target
        self.select = select
        self.dbt_vars = dbt_vars
        self.full_refresh = full_refresh
        self.runner = dbtRunner()

    def execute(self,context: Context) :
        if not os.path.exists(self.dbt_root_dir):
            raise AirflowException(f"dbt_root_dir {self.dbt_root_dir} doest not exists!")
        
        logs_dir = os.path.join(self.dbt_root_dir,"logs")
        if not os.path.exists(logs_dir):
            try:
                os.makedirs(logs_dir,mode=0o777)
                self.log.info(f"Created logs directory: {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to create directory! {logs_dir} : {e}")
                raise AirflowException(f"Failed to create directory! {logs_dir} : {e}")
        if not os.access(logs_dir, os.W_OK):
            try:
                os.chmod(logs_dir, mode = 0o777)
                self.log.info(f"Set writable permissions to logs directory ! {logs_dir}")
            except Exception as e:
                self.log.error(f"Failed to set writable permissions for logs directory : {logs_dir}: {e}")
                raise AirflowException(f"Failed to set writable permissions for logs directory : {logs_dir}: {e}")

        if isinstance(self.dbt_command, str):

            command_parts = self.dbt_command.split()

        else : 
            command_parts = self.dbt_command
        command_parts = self.dbt_command.split()  
        command_args = command_parts + [
            "--project-dir", self.dbt_root_dir,
            "--profiles-dir", self.dbt_root_dir,
        ]

        if self.target:
            command_args.extend(["--target", self.target])

        if self.select:
            command_args.extend(["--select", self.select])

        if self.full_refresh:
            command_args.append(["--full-refresh", self.target])


        if self.dbt_vars:
            vars_string = ' '.join([f"{k} : {v}" for k, v in self.dbt_vars.items()])
            command_args.extend(["--vars", vars_string])

        self.log.info("Executing DBT command: %s", " ".join(command_args))


        res: dbtRunnerResult = self.runner.invoke(command_args)

        if res.success:
            self.log.info('dbt command executed! ')
            if res.result:
                try:
                    for r in res.result:
                        if hasattr(r,'error') and hasattr(r,'status'):
                            self.log.info(f"{r.node.name} : {r.status}")
                        
                except TypeError:
                    self.log.info(f"Completed : result type -> {type(res.result).__name__}")

            else:
                self.log.info('no result returned! ')

        else:
            self.log.error("Error! DBT command Failed! ")