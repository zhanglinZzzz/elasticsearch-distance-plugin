# elasticsearch-distance-plugin

## 支持欧氏距离、余弦距离
## 样例
```http
GET /distance/_search
{
  "query": {
    "function_score": {
      "query": {"match_all": {}},
      "functions": [
        {
          "script_score": {
            "script": {
              "lang": "distance",
              "source": "tianque-script",
              "params": {
                "input":{
                  "x":1,
                  "y":1
                },
                "distance_type":"euclidean",
                "scale":10
              }
            }
          }
        }
      ]
    }
  },
  "sort": [
    {
      "_score": {
        "order": "asc"
      }
    }
  ]
}
```
### 参数说明
| 参数 | 描述 |
| --- | --- |
| input | 输入的向量，至少一个维度，value值可以是整数，也可以是小数 |
| distance_type | 距离类型，支持euclidean(欧式距离，默认)、cosine(余弦距离) |
| scale | 精度，默认值为2 |

### 注意
使用function_score查询而不是script查询，距离字段不会显示在fields中，返回结果的分数_score即为距离，只需要对_score进行排序即可实现按距离排序。
