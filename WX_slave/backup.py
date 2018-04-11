#coding=utf-8
import os
import uuid
import datetime


SOURCE = r"data"
DEST = os.sep.join(['D:','data1','WX_gongzhaonghao'])
UNIQUE_ID = None


def file_handle():
    """file backup script"""
    def is_need_move(file_abs_name):
        # use datetime to get 3-yestoday`s list
        today = datetime.date.today()
        yestoday = str(today - datetime.timedelta(days=1))
        yes_yestoday = str(today - datetime.timedelta(days=2))
        yes_yes_yestoday = str(today - datetime.timedelta(days=3))
        yestodays = [yestoday,yes_yestoday,yes_yes_yestoday]
        for day in yestodays:
            dest_path = os.path.join(DEST,day)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            # if file creat in yestoday or the day 
            # before yestoday,move it to data1
            if day in file_abs_name:
                UNIQUE_ID = get_unique_id()
                new_file_abs_name = file_abs_name.replace('.txt','') + '@'+UNIQUE_ID+'.txt'
                # rename file, add unique id to filename
                os.rename(file_abs_name,new_file_abs_name)
                # move file to destnation
                os.system("move {} {}".format(new_file_abs_name,dest_path))
                return True

    #get file_name`s list
    filename_list = os.listdir(SOURCE)
    for filename in filename_list:
        try:
            # get file`s file_abs_name
            file_abs_name = os.path.join(SOURCE,filename)
            is_need_move(file_abs_name)
        except Exception as e:
            return False

    return True


def get_unique_id():
    """get a unique id """
    global UNIQUE_ID
    if not UNIQUE_ID:
        UNIQUE_ID = str(uuid.uuid1())
    return UNIQUE_ID


if __name__ == '__main__':
    file_handle()
