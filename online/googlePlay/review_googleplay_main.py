import schedule
import time
import requests
import hashlib
import online.const as const
import online.googleplay.review_googleplay_scrap as review_googleplay_scrap
import online.common.review_redis_common_insert as review_redis_common_insert

def check_for_update():
    global prev_hash
    resp = requests.get(const.MNT_PLS_URL, stream=True)
    print("✅headers : ",resp.headers)
    print("✅content tyepe : ",type(resp.content))
    
    # request를 보내서, 변화가 감지되면 아래를 수행
    #review_googleplay_scrap.get_review()
    
def main() :    
    print("running")
    schedule.every(10).seconds.do(check_for_update)

    while True:
        schedule.run_pending()
        time.sleep(1)
        
if __name__ == "__main__":
    main()
