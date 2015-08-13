package com.linkedin.thirdeye.anomaly.lib.fanomaly;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.jblas.DoubleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.exception.FunctionDidNotEvaluateException;

public class StateSpaceAnomalyDetector {

  private final static Logger LOGGER = LoggerFactory.getLogger(StateSpaceAnomalyDetector.class);

  /**
   * We search the previousEstimatedStateNoise +/- this percent
   */
  private static final double ESTIMATE_NOISE_PROP_RANGE_DELTA = 0.25;

  /**
   * If the solution is found within this proportion of the estimated search space, rerun with a larger search space.
   */
  private static final double ESTIMATE_NOISE_PROP_THESHOLD_DELTA = 0.05;

  public long trainStart;
  public long trainEnd;
  public int stepsAhead;
  public long timeGranularity;
  public Set<Long> omitTimestamps;

  private int seasonal;
  private int order;
  private int numStates;
  private int outputStates;
  private double r;

  private Double initialEstimatedStateNoise = null;
  private Double estimatedStateNoise = null;

  /**
   * @return
   *  The estimatedStateNoise afterFineTuning.
   */
  public Double getEstimatedStateNoise() {
    return estimatedStateNoise;
  }

  /**
   * @param estimatedStateNoise
   *  The initial estimate prior to optimization/fine-tuning.
   */
  public void setInitialEstimatedStateNoise(Double initialEstimatedStateNoise) {
    this.initialEstimatedStateNoise = initialEstimatedStateNoise;
  }

  public StateSpaceAnomalyDetector(long trainStartInput, long trainEndInput, int stepsAheadInput,
      long timeGranularityInput, Set<Long> omitTimestampsInput, int seasonalInput, int orderInput,
      int numStatesInput, int outputStatesInput, double rInput)
  {
    trainStart = trainStartInput;
    trainEnd = trainEndInput;
    stepsAhead = stepsAheadInput;
    timeGranularity = timeGranularityInput;
    omitTimestamps = omitTimestampsInput;
    seasonal = seasonalInput;
    order = orderInput;
    numStates = numStatesInput;
    outputStates = outputStatesInput;
    r = rInput;
    LOGGER.warn("Rvalue: " + r);
  }

  public Map<Long, FanomalyDataPoint> ConstantTrainingSequenceCase(DoubleMatrix[] inputTimeSeries,
      long[] inputTimeStamps) throws FunctionDidNotEvaluateException, Exception {

    DoubleMatrix[] trainingTimeSeries = DataUtils.getTrainingData(inputTimeSeries, inputTimeStamps, trainStart,
        trainEnd);
    DoubleMatrix[] processTrainingTimeSeries = DataUtils.removeTimeStamps(trainingTimeSeries, inputTimeStamps,
        omitTimestamps);
    double estimateMean = DataUtils.estimateTrainingMean(processTrainingTimeSeries, seasonal);
    Map<Long, FanomalyDataPoint> output = new HashMap<Long, FanomalyDataPoint>();

    for (int ii = 0; ii < processTrainingTimeSeries.length; ii++ )
    {
      if (processTrainingTimeSeries[ii] == null)
      {
        output.put(inputTimeStamps[ii], new FanomalyDataPoint(Double.NaN, Double.NaN, Double.NaN, 0.0,
            inputTimeStamps[ii], ii));
      }
      else
      {
        if (processTrainingTimeSeries[ii].get(0, 0) == estimateMean)
        {
          output.put(inputTimeStamps[ii], new FanomalyDataPoint(estimateMean, estimateMean, 1.0, 0.0,
              inputTimeStamps[ii], ii));
          /*LOGGER.warning(String.format("%d,%f,%f,%f, %f,%d\n", ii,
              output.get(inputTimeStamps[ii]).actualValue,
              output.get(inputTimeStamps[ii]).predictedValue,
              output.get(inputTimeStamps[ii]).pValue,
              output.get(inputTimeStamps[ii]).stdError,
              output.get(inputTimeStamps[ii]).predictedDate));*/
        }
      }
    }

    return output;
  }

  private StateSpaceModel estimateStateSpaceModel(DoubleMatrix[] inputTimeSeries, long[] inputTimeStamps)
      throws FunctionDidNotEvaluateException, Exception {

    DoubleMatrix[] trainingTimeSeries = DataUtils.getTrainingData(inputTimeSeries, inputTimeStamps, trainStart,
        trainEnd);
    DoubleMatrix[] processTrainingTimeSeries = DataUtils.removeTimeStamps(trainingTimeSeries, inputTimeStamps,
        omitTimestamps);

    if (processTrainingTimeSeries.length <= seasonal) {
      throw new FunctionDidNotEvaluateException("timestamp not long enough for one season.");
    }

    // Kalman here
    DoubleMatrix GG = new DoubleMatrix(numStates, numStates);
    DoubleMatrix FF = new DoubleMatrix(outputStates, numStates);

    for (int ii = 0; ii < order; ii++) {
      GG.put(0, ii, 1);
    }

    if (order == 2) {
      GG.put(1, 1, 1);
    }

    if (seasonal > 0) {
      for (int ii = order; ii < numStates; ii++) {
        GG.put(order, ii, -1);
      }
      for (int ii = order + 1; ii < numStates; ii++) {
        GG.put(ii, (ii-1), 1);
      }
    }

    FF.put(0, 0, 1);
    if (seasonal > 0) {
      FF.put(0, 1, 1);
    }

    double estimate2ndRawMoment= DataUtils.estimateTrainingRawMoment(processTrainingTimeSeries, seasonal, 2);
    double estimateMean = DataUtils.estimateTrainingMean(processTrainingTimeSeries, seasonal);
    LOGGER.warn("estimated input data variance");
    LOGGER.warn(String.format("%f", estimate2ndRawMoment));

    if (estimate2ndRawMoment == 0) {
      // if all constant training sequence, return null object here and
      // no state space model is called.
      throw new FunctionDidNotEvaluateException("no output");
    }

    DoubleMatrix m0 = new DoubleMatrix(numStates, 1);
    m0.put(0, 0, estimateMean);
    DoubleMatrix c0 = DoubleMatrix.eye(numStates).muli(estimate2ndRawMoment);

    BrentOptimizer optimizer = new BrentOptimizer(1e-6, 1e-12);

//    initialEstimatedStateNoise = DataUtils.estimateTrainingVariance(processTrainingTimeSeries, seasonal) / (r + 1);
    System.out.println("initial estimate : " + initialEstimatedStateNoise);
    /*
     * Use the previous solution as a hint to define range.
     */
    if (initialEstimatedStateNoise != null) {
      UnivariatePointValuePair solution = optimizer.optimize(
          new UnivariateObjectiveFunction(new StateSpaceUnivariateObj(GG, FF, r, m0, c0, processTrainingTimeSeries)),
          new MaxEval(100),
          GoalType.MAXIMIZE,
          new SearchInterval((1 - ESTIMATE_NOISE_PROP_RANGE_DELTA) * initialEstimatedStateNoise,
              (1 + ESTIMATE_NOISE_PROP_RANGE_DELTA) * initialEstimatedStateNoise));
      double lowerAcceptabilityThreshold =
          (1 - ESTIMATE_NOISE_PROP_RANGE_DELTA + ESTIMATE_NOISE_PROP_THESHOLD_DELTA) * initialEstimatedStateNoise;
      double upperAcceptabilityThreshold =
          (1 + ESTIMATE_NOISE_PROP_RANGE_DELTA - ESTIMATE_NOISE_PROP_THESHOLD_DELTA) * initialEstimatedStateNoise;
      /*
       * Accept solution if it is not too close to the edge.
       */
      if (solution.getPoint() > lowerAcceptabilityThreshold && solution.getPoint() < upperAcceptabilityThreshold) {
        estimatedStateNoise = solution.getPoint();
      } else {
        LOGGER.warn("the solution from fine-tuning is unsatisfactory");
      }
    }

    if (estimatedStateNoise == null) {
      LOGGER.info("optimizing from scratch");
      UnivariatePointValuePair solution = optimizer.optimize(
          new UnivariateObjectiveFunction(new StateSpaceUnivariateObj(GG, FF, r, m0,c0, processTrainingTimeSeries)),
          new MaxEval(100),
          GoalType.MAXIMIZE,
          new SearchInterval(0.0001, estimate2ndRawMoment));
      estimatedStateNoise = solution.getPoint();
    }

    LOGGER.info("estimatedStateNoise : {}", estimatedStateNoise);

    //construct observation noise matrix and state noise matrix there
    DoubleMatrix StateNoiseMatrix = DoubleMatrix.eye(numStates).muli(estimatedStateNoise);
    DoubleMatrix ObservationNoiseMatrix = DoubleMatrix.eye(outputStates).muli(estimatedStateNoise * r);

    StateSpaceModel Subject = new StateSpaceModel(GG, FF, StateNoiseMatrix,
        ObservationNoiseMatrix, m0, c0, processTrainingTimeSeries);
    return Subject;
  }

  public Map<Long, FanomalyDataPoint> DetectAnomaly(double[] inputData, long[] inputTimeStamps, long offset)
      throws Exception {

    DoubleMatrix[] inputTimeSeries =  new DoubleMatrix[inputData.length];

    for (int ii = 0; ii < inputData.length; ii++)
    {
      if (Double.isNaN(inputData[ii]))
      {
        inputTimeSeries[ii] = null;
      } else {
        inputTimeSeries[ii] = new DoubleMatrix(new double[] {inputData[ii]});
      }
    }

    StateSpaceModel Subject = estimateStateSpaceModel(inputTimeSeries, inputTimeStamps);
    if (Subject == null) {
      Map<Long, FanomalyDataPoint> output = ConstantTrainingSequenceCase(inputTimeSeries, inputTimeStamps);
      return output;
    }

    // output
    Subject.CalculatePrediction(stepsAhead);
    DoubleMatrix[] estimatedMean = Subject.GetEstimatedMeans();
    DoubleMatrix[] estimatedCovariance = Subject.GetEstimatedCovariances();
    NormalDistribution pCal =  new NormalDistribution(0, 1);

    // output P value here
    if (stepsAhead == -1)
    {
      // assuming one dimension here (todo: extend to multi input)
      DoubleMatrix[] trainingSequence = Subject.GetTrainingSequence();

      Map<Long, FanomalyDataPoint> output = new HashMap<Long, FanomalyDataPoint>();
      for (int ii = 0; ii < trainingSequence.length; ii++)
      {
        double MeanTmp;
        if (estimatedMean[ii] != null) {
          MeanTmp = estimatedMean[ii].get(0, 0);
        } else {
          MeanTmp = Double.NaN;
        }

        double VarianceTmp;
        if (estimatedCovariance[ii] != null) {
          VarianceTmp = estimatedCovariance[ii].get(0, 0);
        } else {
          VarianceTmp = Double.NaN;
        }

        if (trainingSequence[ii] == null)
        {
          output.put(inputTimeStamps[ii], new FanomalyDataPoint(MeanTmp, Double.NaN, Double.NaN,Math.sqrt(VarianceTmp),
              inputTimeStamps[ii], ii));
        }
        else
        {
          double Actual = trainingSequence[ii].get(0,0);
          double aPvalue = 1-pCal.cumulativeProbability(Math.abs(Actual-MeanTmp) / Math.sqrt(VarianceTmp));
          output.put(inputTimeStamps[ii], new FanomalyDataPoint(MeanTmp, Actual, aPvalue, Math.sqrt(VarianceTmp),
              inputTimeStamps[ii], ii));
        }
      }
      return output;
    }
    else // stepsAhead != -1
    {
      DoubleMatrix[] predictionSequence = DataUtils.getPredictionData(inputTimeSeries, inputTimeStamps, trainEnd,
          stepsAhead);
      if (predictionSequence == null) {
        throw new FunctionDidNotEvaluateException("no output");
      }

      //double[] pValue = new double[predictionSequence.length];
      Map<Long, FanomalyDataPoint> output = new HashMap<Long, FanomalyDataPoint>();
      long monitorStart = trainEnd + offset;
      for (int ii = 0; ii < predictionSequence.length; ii++)
      {
        double MeanTmp = estimatedMean[ii].get(0, 0);
        double VarianceTmp = estimatedCovariance[ii].get(0, 0);
        double Actual = predictionSequence[ii].get(0,0);
        double aPvalue = 1 - pCal.cumulativeProbability(Math.abs(Actual-MeanTmp) / Math.sqrt(VarianceTmp));

        //pValue[ii] = aPvalue;
        // todo here to fix the date range
        output.put(monitorStart + ii * offset , new FanomalyDataPoint(MeanTmp, Actual, aPvalue,
            Math.sqrt(VarianceTmp), monitorStart + ii * offset, ii));
      }
      return output;
    }
  }

}
