package jd;

import water.*;


import static water.TestUtil.loadAndParseFile;

/**
 * Just a proof of concept to launch some work as a Java App.
 * You need to either compile in to one single jar and use that everywhere, or
 * compile this in a separate jar 
 *
 * --start cluster
 * java -jar h2o-0.0.1.jd.jar
 *
 * --start job.
 * java -cp /Users/jerdavis/devhome/h2otools/repo/h2o/h2o/0.0.1.jd/h2o-0.0.1.jd.jar:/Users/jerdavis/devhome/h2otools/target/h2otools.jar jd.Test
 */
public class Test {

    public static void main(String[] args) throws Exception {
        System.out.println("Hello World");
        Boot.main(Test.class,args);
    }

    public static void userMain(String[] args) {
        System.out.println("User");
        H2O.main(new String[]{});
        System.out.println("User2");
        H2O.waitForCloudSize(2,-1);
        System.out.println("User3");
        try {
            Key k = loadAndParseFile("test.hex","/Users/jerdavis/devhome/h2o.jd/smalldata/cars.csv");
            ValueArray ary = DKV.get(k).get();
            System.out.println(ary);
        }catch(Exception e ) {
            throw new RuntimeException(e);
        }

    }
}
