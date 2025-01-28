#'PT 0H 2M 22S'
import datetime

s = 'PT0H0M0S'
def duration_to_time(s):
    s = s.replace('PT','')
    h = s.split("H")[0]
    m = s.split("H")[1].split("M")[0]
    s = s.split("H")[1].split("M")[1].split("S")[0]
    s_time = datetime.time(hour=int(h), minute=int(m), second=int(s))
    print(h, m, s, s_time)
    return s_time


duration_to_time('PT0H0M0S')
