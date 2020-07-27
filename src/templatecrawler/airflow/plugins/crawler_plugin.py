from airflow.plugins_manager import AirflowPlugin
from templatecrawler.airflow.plugins.operators import SearchRepoOperator#, FilterSearchOperator


class TemplateCrawler(AirflowPlugin):
    name = "templatecrawler"
    operators = [SearchRepoOperator]#, FilterSearchOperator]
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
