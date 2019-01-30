from os.path import expanduser
from gof_Dic import get_dict
from jd.gof_B1 import get_B1
from jd.gof_B2 import get_B2
from jd.gof_B11 import get_B11
from jd.gof_B12A144 import get_B12A144
from jd.gof_B11S1 import get_B11S1
from jd.gof_B59 import get_B59
from jd.gof_B49S2 import get_B49S2
from jd.gof_B49S1 import get_B49S1
from cont.gof_BE1 import get_BE1
from cont.gof_BE5 import get_BE5
from cont.gof_BE7 import get_BE7
from cont.gof_BE70 import get_BE70
from cont.gof_BE23 import get_BE23
from cont.gof_BE7S1 import get_BE7S1
from cont.gof_BE70S1 import get_BE70S1
from cont_total import create_total
from datetime import datetime
import settings


if __name__ == "__main__":
    unloaded = '181126'
    cache = 'Cache'
    path = expanduser('~\Documents')

    connection_str = settings.connection

    rwb = True
    sqlite_rwb_str = f'{path}\{cache}\RWBill.db'
    sqlite_rwbs_str = f'{path}\{cache}\RWBills.db'

    dic = False
    sqlite_dic_str = f'{path}\{cache}\Dic.db'
    sqlite_dics_str = f'{path}\{cache}\Dics.db'

    cnt = False
    sqlite_cont_str = f'{path}\{cache}\Cont.db'
    sqlite_conts_str = f'{path}\{cache}\Conts.db'

    print('started: ', datetime.now().strftime("%I:%M:%S"))

    if dic:
        files = ['B1nsi1', 'B1nsi3', 'B1nsi5', 'B1nsi9', 'BEnsi836', 'B1nsi19']
        for fn in files:
            get_dict(path, unloaded, fn, connection_str, sqlite_dic_str, sqlite_dics_str)

    if rwb:
        get_B1(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)
        get_B2(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)
        get_B49S2(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)
        get_B49S1(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)
        get_B59(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)
        get_B11(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)
        get_B11S1(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)

        # get_B12A144(path, unloaded, connection_str, sqlite_rwb_str, sqlite_rwbs_str)

    if cnt:
        get_BE1(path, unloaded, connection_str, sqlite_cont_str, sqlite_conts_str)
        get_BE5(path, unloaded, connection_str, sqlite_cont_str, sqlite_conts_str)
        get_BE7(path, unloaded, connection_str, sqlite_cont_str, sqlite_conts_str)
        get_BE70(path, unloaded, connection_str, sqlite_cont_str, sqlite_conts_str)
        get_BE23(path, unloaded, connection_str, sqlite_cont_str, sqlite_conts_str)
        get_BE7S1(path, unloaded, connection_str, sqlite_cont_str, sqlite_conts_str)
        get_BE70S1(path, unloaded, connection_str, sqlite_cont_str, sqlite_conts_str)
        create_total(path, unloaded, connection_str, sqlite_conts_str)

    print('ended: ', datetime.now().strftime("%I:%M:%S"))
