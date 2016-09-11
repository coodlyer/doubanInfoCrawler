#coding=utf-8

import urllib2
import httplib
import json,logging
import time
import sys, os,random
from multiprocessing import Pool
reload(sys)
sys.setdefaultencoding("utf8")

def get_proxy(proxy_path):
    proxy_list = open(proxy_path).readlines()
    return [p.strip() for p in proxy_list]


def get_douban_res((data_list, proxies, output_dir, work_num)):
        
    success_fp = open(os.path.join(output_dir, "success_"+work_num), "w")
    failed_fp = open(os.path.join(output_dir, "fail_"+work_num),"w")
    good_proxy = open(os.path.join(output_dir, "good_proxy_"+work_num),"w")
    bad_proxy = open(os.path.join(output_dir, "bad_proxy_"+work_num), "w")

    curr_proxy_pos = 0
    total_proxy_len = len(proxies)
    good_proxy_map = {}
    bad_proxy_map = {}
    last_good_proxy_pos = None

    try:
        for dd in data_list:
            if dd.get("douban_id"):
                logging.info("douban_id %d"%dd["douban_id"])
                go_on = False
                item_try_time = 0
                try_time = 0
                while not go_on:
                    proxys = urllib2.ProxyHandler({'https':proxies[curr_proxy_pos]})
                    opener=urllib2.build_opener(proxys)
                    urllib2.install_opener(opener)
                    try:
                        res = urllib2.urlopen("https://api.douban.com/v2/movie/"+str(dd.get("douban_id")), timeout=2)
                        res = res.read()
                        success_fp.write(str(dd["douban_id"])+"\t"+res+'\n')
                        time.sleep(1.2+random.random())
                        go_on = True
                        if not good_proxy_map.has_key(proxies[curr_proxy_pos]):
                            last_good_proxy_pos = curr_proxy_pos
                            good_proxy_map[proxies[curr_proxy_pos]] = curr_proxy_pos
                            good_proxy.write(proxies[curr_proxy_pos]+"\n")
                            print "good proxy:%s"%proxies[curr_proxy_pos]
                        print "good res"
                    #except httplib.BadStatusLine, e:
                    except Exception, e:
                        print "bad res {%s}"%str(e)
                        time.sleep(1.2+random.random())
                        try_time += 1
                        item_try_time += 1
                        if item_try_time == 12:
                            """ maybe a bad doc, roll back to last good proxy"""
                            failed_fp.write(str(dd.get("douban_id"))+"\n")
                            go_on = True
                            try_time = 0
                            item_try_time = 0
                            if last_good_proxy_pos:
                                curr_proxy_pos = last_good_proxy_pos
                            continue
                        if try_time == 3:
                            """ maybe a bad proxy"""
                            if curr_proxy_pos < total_proxy_len-1:
                                try_time=0
                                if not bad_proxy_map.has_key(proxies[curr_proxy_pos]) and not good_proxy_map.has_key(proxies[curr_proxy_pos]):
                                    bad_proxy_map[proxies[curr_proxy_pos]] = 1
                                    bad_proxy.write(proxies[curr_proxy_pos]+"\n")
                                    print "bad proxy:%s"%proxies[curr_proxy_pos]
                                curr_proxy_pos += 1
                            else:
                                raise "error3 proxy finished" 
                        else:
                            print "try %d/%d time"%(try_time, item_try_time)
                        continue

    except Exception,e:
        print "error {%s}"%str(e)
        success_fp.close()
        failed_fp.close()
        good_proxy.close()
        bad_proxy.close()


if __name__ == "__main__":

    usage = "%s proxy_file douban_json_file concurrency"%__file__
    if len(sys.argv) != 4:
        print usage
        exit(-1)
    
    con = int(sys.argv[3])
    proxies = get_proxy(sys.argv[1])
    p = Pool(con)
    n = len(proxies)/con+1
    proxies = [proxies[i:i+n] for i in xrange(0, len(proxies), n)]
    output_dir = "output/task_"+str(int(time.time()))
    douban_fp = open(sys.argv[2]).readlines()
    data_list = []
    for line in douban_fp:
        try:
            j = json.loads(line.strip())
            data_list.append(j)
        except Exception, e:
            logging.error("error open data file{%s}"%str(e))
            continue
    n = len(data_list)/con+1
    data_list = [data_list[i:i+n] for i in xrange(0, len(data_list), n)] 
    
    params = []
    for i in range(con):
        params.append([data_list[i], proxies[i], output_dir, str(i)])
    os.makedirs(output_dir)
    p.map(get_douban_res, tuple(params))
