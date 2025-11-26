import os
import pickle
import traceback

# Directory containing trained models
model_dir = r"C:\Rethika\Amrita\5th sem\big data analytics\part1\spark_streaming\model"

print("=" * 80)
print("🔍 MODEL DIRECTORY INSPECTION TOOL")
print("=" * 80)

if not os.path.exists(model_dir):
    print(f"❌ Directory does NOT exist: {model_dir}")
    exit(0)

print(f"✅ Directory exists: {model_dir}\n")

# -------------------------------------------------------------------
# 1. List all files and subdirectories
# -------------------------------------------------------------------
print("📁 FILES IN DIRECTORY:")
items = os.listdir(model_dir)
if not items:
    print("   (No files found)")
else:
    for filename in items:
        filepath = os.path.join(model_dir, filename)
        size = os.path.getsize(filepath) if os.path.isfile(filepath) else 0
        if os.path.isfile(filepath):
            print(f"   - {filename:40s} {size:,} bytes")
        else:
            print(f"   - {filename:40s} [DIR]")

print("\n" + "=" * 80)
print("🧪 PICKLE MODEL FILE TESTING")
print("=" * 80)

# -------------------------------------------------------------------
# 2. Test load all .pkl files
# -------------------------------------------------------------------
pkl_files = [f for f in os.listdir(model_dir) if f.endswith('.pkl')]
if not pkl_files:
    print("⚠️  No .pkl files found in directory.")
else:
    for pkl_file in pkl_files:
        file_path = os.path.join(model_dir, pkl_file)
        print(f"\n📦 Attempting to load: {pkl_file}")
        try:
            with open(file_path, 'rb') as f:
                model = pickle.load(f)
            print(f"✅ SUCCESS: Loaded {type(model)}")
            
            # Show key details if available
            if hasattr(model, 'n_estimators'):
                print(f"   → n_estimators: {model.n_estimators}")
            if hasattr(model, 'feature_importances_'):
                print(f"   → feature_importances_: {len(model.feature_importances_)} features")
            if hasattr(model, 'coef_'):
                print(f"   → coef_: {len(model.coef_[0]) if hasattr(model, 'coef_') else 'N/A'}")
            if hasattr(model, 'classes_'):
                print(f"   → classes_: {model.classes_}")
        except Exception as e:
            print(f"❌ FAILED to load: {pkl_file}")
            print(f"   Error: {e}")
            traceback.print_exc(limit=1)

print("\n" + "=" * 80)
print("📦 PYSPARK MODEL DIRECTORY INSPECTION")
print("=" * 80)

# -------------------------------------------------------------------
# 3. Detect and list PySpark model directories
# -------------------------------------------------------------------
spark_dirs = []
for root, dirs, files in os.walk(model_dir):
    if 'metadata' in files and 'data' in dirs:
        spark_dirs.append(root)

if not spark_dirs:
    print("⚠️  No PySpark ML model directories found.")
else:
    for d in spark_dirs:
        print(f"\n✅ Found PySpark model directory:")
        print(f"   Path: {d}")
        for root, dirs, files in os.walk(d):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file), model_dir)
                print(f"     - {rel_path}")

print("\n" + "=" * 80)
print("✅ INSPECTION COMPLETE")
print("=" * 80)
