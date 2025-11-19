from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import requests
import json

app = FastAPI(title="数据集ID获取API", version="1.0.0")


# 响应模型
class ResponseModel(BaseModel):
    code: int
    msg: str
    data: Dict[str, Any]


@app.get("/")
async def root():
    """根路径"""
    return {
        "message": "数据集ID获取API服务运行中",
        "version": "1.0.0",
        "endpoints": {
            "获取数据集ID": "/api/dataset-ids",
            "健康检查": "/health"
        }
    }


def get_response(url, headers, data=None):
    """发送请求并返回解析后的JSON数据"""
    try:
        if data:
            response = requests.post(url, headers=headers, data=data, timeout=10)
        else:
            response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"请求异常: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON解析异常: {e}")
        return None


def extract_ids(response_data):
    """从响应数据中提取items里的所有_id"""
    try:
        if not response_data or response_data.get('code') != 200:
            return []
        data = response_data.get('data', {})
        items = data.get('items', [])
        return [item['_id'] for item in items if '_id' in item]
    except (KeyError, TypeError) as e:
        print(f"提取ID异常: {e}")
        return []


@app.get("/api/dataset-ids", response_model=ResponseModel)
async def get_dataset_ids():
    """获取数据集ID的API接口"""
    base_url = "http://101.52.216.170:32669"
    dataset_path = "/api/core/dataset/pageList"
    full_url = f"{base_url}{dataset_path}"

    headers = {
        "Cookie": "isoftstone_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiI2OGM5Mjk3NzIyYmMyZTE0NDkxNjQ1NWEiLCJ0ZWFtSWQiOiI2OGM5Mjk3NzIyYmMyZTE0NDkxNjQ1NWUiLCJncm91cElkIjoiNjhjOTI5NzcyMmJjMmUxNDQ5MTY0NTYyIiwidG1iSWQiOiI2OGM5Mjk3NzIyYmMyZTE0NDkxNjQ1NjAiLCJnbWJJZCI6IjY4YzkyOTc3MjJiYzJlMTQ0OTE2NDU2NCIsImlzUm9vdCI6dHJ1ZSwiaXNTdXBlciI6dHJ1ZSwicGFzc3dvcmQiOiI4ZDk2OWVlZjZlY2FkM2MyOWEzYTYyOTI4MGU2ODZjZjBjM2Y1ZDVhODZhZmYzY2ExMjAyMGM5MjNhZGM2YzkyIiwiaWF0IjoxNzYyNzU4MzAyfQ.TEya9_qjAwZoTO4oHBZIYmZNBx3uHSUMg8_johjs22Q",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        # 第一步：获取第一层所有ID
        first_layer_data = get_response(full_url, headers)
        if first_layer_data is None:
            return ResponseModel(
                code=500,
                msg="无法连接到数据源",
                data={"items": [], "count": 0}
            )

        first_ids = extract_ids(first_layer_data)

        if not first_ids:
            return ResponseModel(
                code=404,
                msg="第一层无有效ID",
                data={"items": [], "count": 0}
            )

        # 第二步：获取所有第二层ID
        second_ids = []
        for first_id in first_ids:
            second_layer_data = get_response(full_url, headers, data={'parentId': first_id})
            if second_layer_data is not None:
                current_second_ids = extract_ids(second_layer_data)
                if current_second_ids:
                    second_ids.extend(current_second_ids)

        if not second_ids:
            return ResponseModel(
                code=404,
                msg="无有效第二层数据",
                data={"items": [], "count": 0}
            )

        # 第三步：获取第三层所有datasetId并汇总
        total_result = []
        for second_id in second_ids:
            third_layer_data = get_response(full_url, headers, data={'parentId': second_id})
            if third_layer_data is not None:
                third_ids = extract_ids(third_layer_data)
                if third_ids:
                    current_result = [{"datasetId": _id} for _id in third_ids]
                    total_result.extend(current_result)

        return ResponseModel(
            code=200,
            msg=f"成功获取 {len(total_result)} 条数据",
            data={
                "items": total_result,
                "count": len(total_result)
            }
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            msg=f"服务器内部错误: {str(e)}",
            data={"items": [], "count": 0}
        )


@app.get("/health")
async def health_check():
    """健康检查接口"""
    return {"status": "healthy", "service": "dataset-id-api"}

# 注意：删除了uvicorn手动启动代码