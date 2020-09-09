from templatecrawler.logparser.java import JavaParser
import pandas as pd


class CParser(JavaParser):

    _c_functions = {
        'printf': ('format', ['str', '...']),
        'printk': ('format', ['skip', 'str', '...']),
        'fprintf': ('format', ['skip', 'str', '...']),
        'av_log': ('format', ['skip', 'skip', 'str', '...']),
        'log': ('format', ['str', '...']),
        'Log_print': ('format', ['str', '...']),
        'logf': ('format', ['str', '...']),
        'warning': ('format', ['str', '...']),
        'warn': ('format', ['str', '...']),
        'warnx': ('format', ['str', '...']),
        'fatal': ('format', ['str', '...']),
        'dfatal': ('format', ['str', '...']),
        'debug': ('format', ['skip', 'str', '...']),
        'LOG_ERR': ('format', ['str', '...']),
        'GX_LOG': ('format', ['str', '...']),
        'vcos_log_error': ('format', ['str', '...']),
        'vcos_log_warn': ('format', ['str', '...']),
        'vcos_log_info': ('format', ['str', '...']),
        'vcos_log_trace': ('format', ['str', '...']),
        'vcos_logc_error': ('format', ['str', '...']),
        'vcos_logc_warn': ('format', ['str', '...']),
        'vcos_logc_info': ('format', ['str', '...']),
        'vcos_logc_trace': ('format', ['str', '...']),
        'GIMP_LOG': ('format', ['skip', 'str', '...']),
        'Critf': ('format', ['str', '...']),
        'Infof': ('format', ['str', '...']),
        'Warningf': ('format', ['str', '...']),
        'Tracef': ('format', ['str', '...']),
        'Debugf': ('format', ['str', '...']),
        'Errf': ('format', ['str', '...']),
        'Crit': ('format', ['str', '...']),
        'Info': ('format', ['str', '...']),
        'Warning': ('format', ['str', '...']),
        'Trace': ('format', ['str', '...']),
        'Debug': ('format', ['str', '...']),
        'Err': ('format', ['str', '...']),
        'g_log': ('format', ['skip', 'skip','str', '...']),
        'srm_printk': ('format', ['str', '...']),
        'pr_warn': ('format', ['str', '...']),
        'pr_debug': ('format', ['str', '...']),
        'dprintk': ('format', ['str', '...']),
    }

    def __init__(self, framework: str):
        super().__init__(framework)

        self._framework_map = self._c_functions

    def run(self, data: pd.Series):
        data = data.apply(str.strip)
        mask = data.str.startswith('#')
        data = data[~mask]
        return super(CParser, self).run(data)
