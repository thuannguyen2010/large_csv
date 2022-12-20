import os
import time


def str_time_prop(start, end, time_format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


if __name__ == '__main__':
    import random

    outfile = 'inputs/songs_1_Mi.csv'
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    outsize = 1024 * 1024 * 1  # 1MB
    songs = [f'song{str(i)}' for i in range(1000)]


    def random_date(start, end, prop) -> str:
        return str_time_prop(start, end, '%Y-%m-%d', prop)


    with open(outfile, 'ab') as csvfile:
        size = 0
        csvfile.write("Song,Date,Number of Plays\n".encode())
        while size < outsize:
            txt = f"{random.choice(songs)},{random_date('2022-01-01', '2022-12-31', random.random())},1\n"
            size += len(txt)
            csvfile.write(txt.encode())
