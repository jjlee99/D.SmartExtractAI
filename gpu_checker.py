import torch
print("GPU 사용 가능 여부:", torch.cuda.is_available())
print("GPU 이름:", torch.cuda.get_device_name(0))
print("GPU 메모리(GB):", torch.cuda.get_device_properties(0).total_memory / 1e9)
