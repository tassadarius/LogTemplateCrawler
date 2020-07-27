import setuptools


setuptools.setup(
    name='template-crawler',
    version='0.2.0',
    description='GitHub-Crawler which extracts log templates',
    package_dir={'': 'src'},
    packages=setuptools.find_packages('src'),
    entry_points={
       'airflow.plugins': [
           'crawler_plugin = templatecrawler.airflow.plugins.crawler_plugin:TemplateCrawler'
       ]
   }
)
