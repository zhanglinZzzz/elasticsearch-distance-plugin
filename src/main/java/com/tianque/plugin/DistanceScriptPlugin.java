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
import java.util.Collection;
import java.util.HashMap;
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

                    private final Map<String, Object> input;
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
                                    "Parse [scale] error, the value of field [scale] must be a integer");
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
                                for (String field : input.keySet()) {
                                    if (!lookup.source().containsKey(field)) {
                                        throw new IllegalArgumentException("Cannot find the field [" + field + "]");
                                    }
                                }
                                Map<String, Object> target = new HashMap<String, Object>();
                                for (String field : input.keySet()) {
                                    Object value = lookup.source().get(field).toString();
                                    target.put(field, value);
                                }
                                BigDecimal result;
                                if (EUCLIDEAN_DISTANCE.equals(distanceType)) {
                                    result = calculateEuclideanDistance(input, target);
                                } else {
                                    result = calculateCosineDistance(input, target);
                                }
                                return result.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
                            }

                            /**
                             * 计算欧氏距离
                             * @param input
                             * @param target
                             * @return
                             */
                            private BigDecimal calculateEuclideanDistance(Map<String, Object> input,
                                Map<String, Object> target) {
                                BigDecimal result = BigDecimal.ZERO;
                                for (String field : input.keySet()) {
                                    BigDecimal inputValue = new BigDecimal(input.get(field).toString());
                                    BigDecimal targetValue = new BigDecimal(target.get(field).toString());
                                    result = result.add(inputValue.subtract(targetValue).pow(2));
                                }
                                return new BigDecimal(Math.sqrt(result.doubleValue()));
                            }

                            /**
                             * 计算余弦距离
                             * @param input
                             * @param target
                             * @return
                             */
                            private BigDecimal calculateCosineDistance(Map<String, Object> input,
                                Map<String, Object> target) {
                                BigDecimal numerator = BigDecimal.ZERO;
                                BigDecimal inputDenominator = BigDecimal.ZERO;
                                BigDecimal targetDenominator = BigDecimal.ZERO;
                                for (String field : input.keySet()) {
                                    BigDecimal inputValue = new BigDecimal(input.get(field).toString());
                                    BigDecimal targetValue = new BigDecimal(target.get(field).toString());
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

                };
                return context.factoryClazz.cast(factory);
            } else {
                throw new IllegalArgumentException("Unknown script name [" + scriptSource + "]");
            }
        }
    }
}
