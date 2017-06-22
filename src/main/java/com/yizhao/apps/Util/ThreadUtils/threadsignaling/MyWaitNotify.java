package com.yizhao.apps.Util.ThreadUtils.threadsignaling;

/**
 * Reference:
 * http://tutorials.jenkov.com/java-concurrency/thread-signaling.html
 *
 * This class is the perfect example for thread signaling, it takes care the following potential bugs:
 *
 * 1: Missed Signals
 *      To avoid losing signals they should be stored inside the signal class.
 *      In the MyWaitNotify example the notify signal should be stored in a member variable inside the MyWaitNotify instance.
 *      Here is a modified version of MyWaitNotify that does this:
 *      Notice how the doNotify() method now sets the wasSignalled variable to true before calling notify().
 *      Also, notice how the doWait() method now checks the wasSignalled variable before calling wait().
 *      In fact it only calls wait() if no signal was received in between the previous doWait() call and this.
 *
 * 2: Spurious Wakeups
 *      For inexplicable reasons it is possible for threads to wake up even if notify() and notifyAll() has not been called. This is known as spurious wakeups. Wakeups without any reason.
 *      To guard against spurious wakeups the signal member variable is checked inside a while loop instead of inside an if-statement. Such a while loop is also called a spin lock. The thread awakened spins around until the condition in the spin lock (while loop) becomes false.
 * 3: Don't call wait() on constant String's or global objects
 *      We use MonitorObject myMonitorObject object, instead of a String myMonitorObject = ""
 *      The problem with calling wait() and notify() on the empty string, or any other constant string is, that the JVM/Compiler internally translates constant strings into the same object.
 *      That means, that even if you have two different MyWaitNotify instances, they both reference the same empty string instance.
 *      This also means that threads calling doWait() on the first MyWaitNotify instance risk being awakened by doNotify() calls on the second MyWaitNotify instance.
 *      So: Don't use global objects, string constants etc. for wait() / notify() mechanisms. Use an object that is unique to the construct using it.
 */
public class MyWaitNotify {
    MonitorObject myMonitorObject = new MonitorObject(); // So: Don't use global objects, string constants etc. for wait() / notify() mechanisms. Use an object that is unique to the construct using it. like MonitorObject Class
    boolean wasSignalled = false;

    public void doWait(){
        synchronized(myMonitorObject){
            // wasSignalled object fixed for missed signals
            /**
             * while loop fixed for Spurious Wakeups
             * For inexplicable reasons it is possible for threads to wake up even if notify() and notifyAll() has not been called.
             * This is known as spurious wakeups. Wakeups without any reason.
             * To guard against spurious wakeups the signal member variable is checked inside a while loop instead of inside an if-statement.
             * Such a while loop is also called a spin lock. The thread awakened spins around until the condition in the spin lock (while loop) becomes false.
             */
            while(!wasSignalled){
                try{
                    myMonitorObject.wait();
                } catch(InterruptedException e){

                }
            }
            //clear signal and continue running.
            wasSignalled = false;
        }
    }

    public void doNotify(){
        synchronized(myMonitorObject){
            wasSignalled = true; // wasSignalled object fixed for missed signals
            myMonitorObject.notify();
        }
    }
}
