package jd;

import hex.FrameTask;
import hex.glm.GLM2;
import hex.glm.GLMModel;
import hex.glm.GLMParams;
import water.*;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;
import water.fvec.Vec;
import water.util.Log;


import java.io.File;
import java.util.*;

import static java.util.Arrays.asList;
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

        logOff();


        Boot.main(Test.class,args);
    }

    public static void logOff() {
        Log.unsetFlag(Log.Tag.Sys.CLEAN);
        Log.unsetFlag(Log.Tag.Sys.CONFM);
        Log.unsetFlag(Log.Tag.Sys.DRF__);
        Log.unsetFlag(Log.Tag.Sys.EXCEL);
        Log.unsetFlag(Log.Tag.Sys.GBM__);
        Log.unsetFlag(Log.Tag.Sys.GENLM);
        Log.unsetFlag(Log.Tag.Sys.HDFS_);
        Log.unsetFlag(Log.Tag.Sys.HTTPD);
        Log.unsetFlag(Log.Tag.Sys.KMEAN);
        Log.unsetFlag(Log.Tag.Sys.LOCKS);
        Log.unsetFlag(Log.Tag.Sys.PARSE);
        Log.unsetFlag(Log.Tag.Sys.RANDF);
        Log.unsetFlag(Log.Tag.Sys.SCORM);
        Log.unsetFlag(Log.Tag.Sys.STORE);
        Log.unsetFlag(Log.Tag.Sys.WATER);
    }

    @SuppressWarnings ("unused")
    public static void userMain(String[] args) {
        System.out.println("User");

        logOff();

        H2O.main(new String[]{});
        System.out.println("User2");
        H2O.waitForCloudSize(1,-1);
        System.out.println("User3");

        logOff();

        foo();
        //bar();

//
//
//        try {
//            Key k = loadAndParseFile("test.hex","/Users/jerdavis/devhome/h2o.jd/smalldata/cars.csv");
//
//            ValueArray ary = DKV.get(k).get();
//            System.out.println(ary);
//        }catch(Exception e ) {
//            throw new RuntimeException(e);
//        }

    }

    private static void foo() {

        Key      parsed   = Key.make("cars_parsed");
        GLMModel model    = null;
        //String response = "power (hp)";
        String response = "is_purchase";



        //List<Frame> uniFrames = getUniFrames(parsed, "/Users/jerdavis/devhome/h2o.jd/smalldata/cars.csv", new HashSet<String>(asList("name")), response);
        List<Frame> uniFrames = getUniFrames(parsed, "/Users/jerdavis/temp/pbdtc/part-00101.gz", new HashSet<String>(asList("subject","date","is_responder","sales")), response);
        List<GLM2>   glmTasks  = new ArrayList<GLM2>();
        List<String> names     = new ArrayList<String>();
        System.out.println("Got:" + uniFrames.size() + " Univariate Frames");

        for(Frame frame : uniFrames ) {
            Key jobKey   = Key.make();
            Key modelKey = Key.make();
            FrameTask.DataInfo dinfo = new FrameTask.DataInfo(frame,
                                                              1,
                                                              true);

            GLMParams glm = new GLMParams( GLMParams.Family.gaussian,
                                           0,
                                           GLMParams.Family.gaussian.defaultLink,
                                           0 );

            GLM2 glmTask = new GLM2("Univar test",
                         jobKey,
                         modelKey,
                         dinfo,
                         glm,
                         new double[]{0},0).fork();
            glmTasks.add(glmTask);
            names.add(frame._names[0]);
        }
        List<SubResult> subResults = new ArrayList<SubResult>();
        for( int x=0;x<glmTasks.size();x++) {
            GLM2 task = glmTasks.get(x);
            model = task.get();
            //model = DKV.get(modelKey).get();
            //model.
//            HashMap<String,Double> coefs = model.coefficients();
//            System.out.println(coefs);

            //Looks like intercept is the last term
            SubResult subResult = new SubResult(names.get(x),
                                                model.beta()[1],
                                                model.beta()[0],
                                                model.norm_beta()[1],
                                                model.norm_beta()[0],
                                                model.aic(),
                                                model.auc(),
                                                model.devExplained()
            );
            subResults.add(subResult);
            model.delete();
        }
        Collections.sort(subResults, new Comparator<SubResult>() {
            @Override
            public int compare (SubResult thisResult, SubResult thatResult) {
                return( Double.compare(thisResult.devExplained,thatResult.devExplained) );
            }
        });
        for(SubResult result : subResults) {
            System.out.println(result);
        }
    }

    private static void bar() {
        Key parsed = Key.make("cars_parsed");
        Key modelKey = Key.make("cars_model");
        Frame fr = null;
        GLMModel model = null;

        String [] ignores = new String[]{"name"};
        String response = "power (hp)";
        fr = getFrameForFile(parsed, "/Users/jerdavis/devhome/h2o.jd/smalldata/cars.csv", ignores, response);
        FrameTask.DataInfo dinfo = new FrameTask.DataInfo(fr, 1, true);
        GLMParams glm = new GLMParams(GLMParams.Family.poisson,0, GLMParams.Family.poisson.defaultLink,0);
        new GLM2("GLM test on cars.",Key.make(),modelKey,dinfo,glm,new double[]{0},0).fork().get();
        model = DKV.get(modelKey).get();
        HashMap<String,Double> coefs = model.coefficients();
        System.out.println(coefs);
        model.delete();

    }

    private static Frame getFrameForFile(Key outputKey, String path,String [] ignores, String response){
      File f = TestUtil.find_test_file(path);
      Key k = NFSFileVec.make(f);
      Frame fr = ParseDataset2.parse(outputKey, new Key[]{k});
      if(ignores != null)
        for(String s:ignores) UKV.remove(fr.remove(s)._key);
      // put the response to the end
      fr.add(response, fr.remove(response));
      return fr;
    }

    private static List<Frame> getUniFrames(Key outputKey,
                                            String path,
                                            Set<String> ignores,
                                            String response){
      List<Frame> results = new ArrayList<Frame>();
      File     f           = TestUtil.find_test_file(path);
      Key      k           = NFSFileVec.make(f);
      Frame    fr          = ParseDataset2.parse(outputKey, new Key[]{k});
      Map<String,Vec> vecMap = getVecMap(fr);
      Vec      responseVec = vecMap.get(response);

      for(Map.Entry<String,Vec> entry : vecMap.entrySet() ) {
          String name = entry.getKey();

          if( !name.equals(response) && !ignores.contains(name) ) {
            Vec depVec = entry.getValue();

              //if( depVec.sigma() == 0  ||  depVec.isEnum() ) {
              if( depVec.sigma() == 0 || depVec.naCnt() > 0  ||  depVec.isEnum() ) {
                System.out.println("SKip:"+name);
              }else {
                results.add(new Frame(new String[]{name,response}, new Vec[]{depVec,responseVec}) );
              }
          }
      }
      return( results );
    }

    public static Map<String,Vec> getVecMap(Frame frame) {
        Map<String,Vec> result = new HashMap<String,Vec>();
        Vec[] vecs = frame.vecs();
        String[] names = frame.names();
        for( int x=0;x<names.length;x++) {
            result.put(names[x],vecs[x]);
        }
        return( result );
    }

    public static class SubResult {
        private final String name;
        private final double intercept;
        private final double beta;
        private final double normIntercept;
        private final double normBeta;
        private final double aic;
        private final double auc;
        private final double devExplained;

        public SubResult (String name, double intercept, double beta, double normIntercept, double normBeta, double aic,
                          double auc, double devExplained) {
            this.name = name;
            this.intercept = intercept;
            this.beta = beta;
            this.normIntercept = normIntercept;
            this.normBeta = normBeta;
            this.aic = aic;
            this.auc = auc;
            this.devExplained = devExplained;
        }

        @Override
        public String toString () {
            return "SubResult{" +
                   "name='" + name + '\'' +
                   ", intercept=" + intercept +
                   ", beta=" + beta +
                   ", normIntercept=" + normIntercept +
                   ", normBeta=" + normBeta +
                   ", aic=" + aic +
                   ", auc=" + auc +
                   ", devExplained=" + devExplained +
                   '}';
        }
    }

}

