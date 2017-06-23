package com.yizhao.apps.Util.ThreadUtils.threadsignaling;

public class MyWaitNotify {
    MonitorObject myMonitorObject = new MonitorObject();
    boolean wasSignalled = false;

    public void doWait() {
        synchronized (myMonitorObject) {
            while (!wasSignalled) {
                System.out.println(Thread.currentThread().getName() + ": waiting...");
                try {
                    myMonitorObject.wait();
                } catch (InterruptedException e) {

                }
            }
            System.out.println(Thread.currentThread().getName() + ": done job...");
            wasSignalled = false;
        }
    }

    public void doNotify() {
        synchronized (myMonitorObject) {
            wasSignalled = true;
            myMonitorObject.notify();
            System.out.println(Thread.currentThread().getName() + ": notify single thread...");
        }
    }


    public void doNotifyAll() {
        synchronized (myMonitorObject) {
            wasSignalled = true;
            myMonitorObject.notifyAll();
            System.out.println(Thread.currentThread().getName() + ": notify all threads...");
        }
    }
}
