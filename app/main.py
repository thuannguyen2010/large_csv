import sys

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/file-upload")
async def root():
    return {"requestId": "requestId"}


if __name__ == "__main__":
    import random
    import csv
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


    def random_date(start, end, prop) -> str:
        return str_time_prop(start, end, '%Y-%m-%d', prop)


    outfile = 'songs.csv'
    outsize = 8 * 1024 * 1024 * 6400  # 2.4*6400MB
    with open(outfile, 'w', encoding='utf-8') as csvfile:
        size = 0
        writer = csv.writer(csvfile)
        writer.writerow(["Song", "Date", "Number of Plays"])
        songs = [f'song{str(i)}' for i in range(1000)]
        while size < outsize:
            row = [random.choice(songs), random_date('2022-01-01', '2022-12-31', random.random()),
                   1]
            size += sys.getsizeof(row)
            writer.writerow(row)
