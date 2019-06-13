package com.tianque.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 计算距离
 *
 * @author zhanglin
 * @date 2019-6-3 15:34
 */
public class DistanceScriptPlugin extends Plugin implements ScriptPlugin {

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new DistanceScriptEngine();
    }

    private static class DistanceScriptEngine implements ScriptEngine {

        private static final String SCRIPT_SOURCE = "tianque-script";
        private static final String SCRIPT_LANG = "distance";

        @Override
        public String getType() {
            return SCRIPT_LANG;
        }

        @Override
        public <FactoryType> FactoryType compile(String name, String scriptSource, ScriptContext<FactoryType> context,
            Map<String, String> params) {
            if (SCRIPT_SOURCE.equals(scriptSource)) {
                ScoreScript.Factory factory = (p, lookup) -> new ScoreScript.LeafFactory() {

                    private static final String EUCLIDEAN_DISTANCE = "euclidean";
                    private static final String COSINE_DISTANCE = "cosine";
                    private static final String DEFAULE_SEPARATOR = ",";

                    private final Map<String, Object> input;
                    private final List<Integer> inputValueList;
                    private final String separator;
                    private final String distanceType;
                    private final Integer scale;

                    {
                        if (!p.containsKey("input")) {
                            throw new IllegalArgumentException("Missing parameter [field]");
                        }
                        try {
                            input = (Map<String, Object>)p.get("input");
                        } catch (Exception e) {
                            throw new IllegalArgumentException("Parse [input] error");
                        }
                        if (input == null || input.size() == 0) {
                            throw new IllegalArgumentException("Input is empty");
                        }
                        if (input.size() != 1) {
                            throw new IllegalArgumentException(
                                "The number of key-value mappings in the [input] is not one");
                        }
                        if (p.containsKey("separator")) {
                            separator = p.get("separator").toString();
                        } else {
                            separator = DEFAULE_SEPARATOR;
                        }
                        if (separator == null || separator.length() == 0) {
                            throw new IllegalArgumentException("The [separator] cannot be ''");
                        }
                        try {
                            String key = input.keySet().iterator().next();
                            inputValueList = string2List(input.get(key).toString(), separator);
                        } catch (Exception e) {
                            throw new IllegalArgumentException("Parse [input] error, check your [input] or [separator]",
                                e);
                        }
                        if (inputValueList == null || inputValueList.size() == 0) {
                            throw new IllegalArgumentException(
                                "Parse [input] error, check your [input] or [separator]");
                        }
                        if (p.containsKey("distance_type")) {
                            distanceType = p.get("distance_type").toString();
                            if (!EUCLIDEAN_DISTANCE.equals(distanceType) && !COSINE_DISTANCE.equals(distanceType)) {
                                throw new IllegalArgumentException(
                                    "Parse [distance_type] error, the value of field [distance_type] must be 'euclidean' or 'cosine'");
                            }
                        } else {
                            distanceType = EUCLIDEAN_DISTANCE;
                        }
                        if (!p.containsKey("scale")) {
                            scale = 2;
                        } else {
                            try {
                                scale = Integer.parseInt(p.get("scale").toString());
                            } catch (Exception e) {
                                throw new IllegalArgumentException(
                                    "Parse [scale] error, the value of field [scale] must be a integer", e);
                            }
                        }
                        if (scale < 0) {
                            throw new IllegalArgumentException(
                                "Parse [scale] error, the value of field [scale] must be a nonnegative integer");
                        }
                    }

                    @Override
                    public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
                        return new ScoreScript(p, lookup, ctx) {
                            @Override
                            public double execute() {
                                String field = input.keySet().iterator().next();
                                if (!lookup.source().containsKey(field)) {
                                    throw new IllegalArgumentException("Cannot find the field [" + field + "]");
                                }
                                String value = lookup.source().get(field).toString();
                                List<Integer> targetValueList = string2List(value, separator);
                                if (inputValueList.size() != targetValueList.size()) {
                                    throw new IllegalArgumentException(
                                        "The length of [" + field + "] you enter is not equal to the length of ["
                                            + field + "] in documents");
                                }
                                BigDecimal result;
                                if (EUCLIDEAN_DISTANCE.equals(distanceType)) {
                                    result = calculateEuclideanDistance(inputValueList, targetValueList);
                                } else {
                                    result = calculateCosineDistance(inputValueList, targetValueList);
                                }
                                return result.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
                            }

                            /**
                             * 计算欧氏距离
                             * @param inputValueList
                             * @param targetValueList
                             * @return
                             */
                            private BigDecimal calculateEuclideanDistance(List<Integer> inputValueList,
                                List<Integer> targetValueList) {
                                BigDecimal result = BigDecimal.ZERO;
                                for (int i = 0; i < inputValueList.size(); i++) {
                                    BigDecimal inputValue = new BigDecimal(inputValueList.get(i));
                                    BigDecimal targetValue = new BigDecimal(targetValueList.get(i));
                                    result = result.add(inputValue.subtract(targetValue).pow(2));
                                }
                                return new BigDecimal(Math.sqrt(result.doubleValue()));
                            }

                            /**
                             * 计算余弦距离
                             * @param inputValueList
                             * @param targetValueList
                             * @return
                             */
                            private BigDecimal calculateCosineDistance(List<Integer> inputValueList,
                                List<Integer> targetValueList) {
                                BigDecimal numerator = BigDecimal.ZERO;
                                BigDecimal inputDenominator = BigDecimal.ZERO;
                                BigDecimal targetDenominator = BigDecimal.ZERO;
                                for (int i = 0; i < inputValueList.size(); i++) {
                                    BigDecimal inputValue = new BigDecimal(inputValueList.get(i));
                                    BigDecimal targetValue = new BigDecimal(targetValueList.get(i));
                                    numerator = numerator.add(inputValue.multiply(targetValue));
                                    inputDenominator = inputDenominator.add(inputValue.pow(2));
                                    targetDenominator = targetDenominator.add(targetValue.pow(2));
                                }
                                inputDenominator = new BigDecimal(Math.sqrt(inputDenominator.doubleValue()));
                                targetDenominator = new BigDecimal(Math.sqrt(targetDenominator.doubleValue()));
                                if (BigDecimal.ZERO.equals(inputDenominator) || BigDecimal.ZERO
                                    .equals(targetDenominator)) {
                                    throw new IllegalArgumentException(
                                        "Calculate cosine distance error, there are origins in the documents and the input");
                                }
                                return numerator.divide(inputDenominator.multiply(targetDenominator), scale,
                                    BigDecimal.ROUND_HALF_UP);
                            }

                        };
                    }

                    @Override
                    public boolean needs_score() {
                        return false;
                    }

                    private List<Integer> string2List(String str, String separator) {
                        List<Integer> list = new ArrayList<Integer>();
                        if (str != null && str.length() > 0) {
                            String[] values = str.split(separator);
                            for (String value : values) {
                                Integer valueInteger = Integer.parseInt(value.trim());
                                list.add(valueInteger);
                            }
                        }
                        return list;
                    }
                };
                return context.factoryClazz.cast(factory);
            } else {
                throw new IllegalArgumentException("Unknown script name [" + scriptSource + "]");
            }
        }
    }
}
