package fr.liglab.sami.utilities;

class StopWatch {

    var startTime: Long = 0;
    var stopTime: Long = 0;
    var running: Boolean = false;


    def start() {
        startTime = System.currentTimeMillis();
        running = true;
    }


    def stop() {
        stopTime = System.currentTimeMillis();
        running = false;
    }


    //elaspsed time in milliseconds
    def getElapsedTime(): Long =  {
        var elapsed: Long = 0L;
        if (running) {
             elapsed = (System.currentTimeMillis() - startTime);
        }
        else {
            elapsed = (stopTime - startTime);
        }
        return elapsed;
    }


    //elaspsed time in seconds
    def getElapsedTimeSecs(): Long =  {
        var elapsed: Long = 0L;
        if (running) {
            elapsed = ((System.currentTimeMillis() - startTime) / 1000);
        }
        else {
            elapsed = ((stopTime - startTime) / 1000);
        }
        return elapsed;
    }

    //sample usage
    object StopWatchRunner {
      def main(args: Array[String]): Unit = {
        var sw: StopWatch = new StopWatch()
        sw.start();
        sw.stop();
        println("elapsed time in milliseconds: " + sw.getElapsedTime())
      }
    }
}
