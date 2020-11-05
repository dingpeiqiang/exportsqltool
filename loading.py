import time
import os

load = False #是否开启正在loading
savetext = 'loading.'
currentobj = None
import time
import _thread


def loading_action(obj):
    list_b = ['loading.', 'loading..', 'loading...', 'loading....', 'loading    ']
    while True:
        if not load:
            break
        for i in list_b:
            currentobj.export_button.setText(i)
            #print('%s\r' % i, end='')
            time.sleep(0.2)

def loading(obj):
    global  savetext
    global currentobj
    global  load
    load =True
    currentobj = obj
    savetext = obj.export_button.text()
    _thread.start_new_thread(loading_action,(currentobj,))

def loading_close():
    global  load
    load = False
    time.sleep(1)
    currentobj.export_button.setText(savetext)


if __name__ =="__main__":
    loading()
    time.sleep(5)
    loading_close()