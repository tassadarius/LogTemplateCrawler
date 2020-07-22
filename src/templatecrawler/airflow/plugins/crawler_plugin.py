from airflow.plugins_manager import AirflowPlugin


class TemplateCrawler(AirflowPlugin):
    name = "templatecrawler"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
