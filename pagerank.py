from google.cloud import storage
import numpy as np
from numpy import linalg as LA
import os 
import re 
import concurrent.futures
import tqdm



def calc_stats(outmatrix):
     
    inmatrix = np.transpose(outmatrix)

    out_mean = np.mean(np.sum(outmatrix, axis=1))
    out_median = np.median(np.sum(outmatrix, axis=1))
    out_max = np.max(np.sum(outmatrix, axis=1))
    out_min = np.min(np.sum(outmatrix, axis=1))
    out_quintiles = np.quantile(np.sum(outmatrix, axis=1), [0.2, 0.4, 0.6, 0.8, 1.0] )

    in_mean = np.mean(np.sum(inmatrix, axis=1))
    in_median = np.median(np.sum(inmatrix, axis=1))
    in_max = np.max(np.sum(inmatrix, axis=1))
    in_min = np.min(np.sum(inmatrix, axis=1))
    in_quintiles = np.quantile(np.sum(inmatrix, axis=1), [0.2, 0.4, 0.6, 0.8, 1.0] )

    print("average of outgoing links:", out_mean)
    print("median of outgoing links:", out_median)
    print("max of outgoing links:", out_max)
    print("min of outgoing links:", out_min)
    print("quintiles of outgoing links:", out_quintiles)

    print()
    print("average of incoming links:", in_mean)
    print("median of incoming links:", in_median)
    print("max of incoming links:", in_max)
    print("min of incoming links:", in_min)
    print("quintiles of incoming links:", in_quintiles)


def pagerank(outmatrix):
    num_V = len(outmatrix)
    prev = np.zeros(num_V)

    ins = [np.where(outmatrix[:, i] == 1)[0] for i in range(num_V)]
    outs = [outmatrix[i].sum(axis=1) for i in ins]

    while True:
        current = prev.copy()

        offset = np.array([np.sum(current[ins[i]] / outs[i])for i in range(num_V)])

        current = 0.15 + 0.85 * offset

        err = LA.norm(current - prev)
        err = np.abs(err) 

        prev = current

        if err < 0.005:
            break

    prev = prev / np.sum(prev)
    return prev



def top_pagerank(outmatrix):
    pr = pagerank(outmatrix)
    top5 = sorted(range(len(pr)), key=lambda x: pr[x], reverse=True)[:5]
    for i in top5:
        str_pr5 = "page " + str(i) + ": " + str(pr[i])
        print(str_pr5)
        


def read_blob(blob, outmatrix):
    filename = int(blob.name.split(".")[0].split("/")[1])

    content = blob.download_as_string().decode("utf-8")
    
    links = re.findall(r'<a HREF="(\d+).html">', content)

    for link in links:
        outmatrix[filename, int(link)] = 1
    
    return outmatrix



def read_blobs(blobs, outmatrix, num_blobs):
    num_workers = os.cpu_count()*5
    with tqdm(total=num_blobs) as progress:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:

            iterator = executor.map(read_blob, (blobs, outmatrix))
            for item in iterator:
                outmatrix = item
                progress.update(1)
                 
            


def main():
    storage_client = storage.Client().create_anonymous_client()
    bucket = storage_client.bucket("ds561-tigeryi-hw2")
    blobs = bucket.list_blobs(prefix="files")
    num_blobs = sum(1 for blob in blobs)
    outmatrix = np.zeros((num_blobs, num_blobs))

    read_blobs(blobs, outmatrix, num_blobs)
    calc_stats(outmatrix)
    top_pagerank(outmatrix)


if __name__ == "__main__":
    main()



