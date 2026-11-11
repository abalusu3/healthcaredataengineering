"""Resolve Spark extension jars (with transitive deps) at image build time and copy them into
pyspark's jars/ dir, so runtime never touches Maven and driver + executors share one classpath.

Used by BOTH the Spark image and the Airflow image (the driver runs inside Airflow).
"""

import glob
import os
import re
import shutil
import tempfile
import urllib.request

import pyspark
from pyspark.sql import SparkSession

PACKAGES = os.environ.get(
    "SPARK_EXTRA_PACKAGES",
    ",".join(
        [
            "io.delta:delta-spark_4.1_2.13:4.4.1",
            "org.apache.hadoop:hadoop-aws:3.4.2",  # must equal pyspark's bundled Hadoop
            "org.postgresql:postgresql:42.7.8",
        ]
    ),
)

jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
bundled_hadoop = [f for f in os.listdir(jars_dir) if f.startswith("hadoop-client-api-")]
print(f"pyspark {pyspark.__version__}; bundled: {bundled_hadoop}")

ivy = tempfile.mkdtemp()
spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.jars.packages", PACKAGES)
    .config("spark.jars.ivy", ivy)
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.stop()


def artifact(name: str) -> str:
    return re.sub(r"-\d[\w.\-]*\.jar$", "", name)


existing = {artifact(f) for f in os.listdir(jars_dir)}
copied = 0
for jar in glob.glob(os.path.join(ivy, "jars", "*.jar")):
    name = os.path.basename(jar)
    # Ivy names jars "<groupId>_<artifactId>-<version>.jar"; drop the groupId prefix.
    group, _, rest = name.partition("_")
    stem = rest if rest and "." in group else name
    if artifact(stem) in existing:
        print(f"skip (already bundled): {name}")
        continue
    shutil.copy(jar, os.path.join(jars_dir, stem))
    copied += 1
    print(f"added: {stem}")
print(f"copied {copied} jars into {jars_dir}")
shutil.rmtree(ivy, ignore_errors=True)

# Transitive deps can drag in Jackson modules older than Spark's own Jackson, which fails at
# runtime with module/databind version mismatches. Re-pin them to Spark's databind version.
databind = next(f for f in os.listdir(jars_dir) if f.startswith("jackson-databind-"))
jackson_version = databind[len("jackson-databind-") : -len(".jar")]
for f in os.listdir(jars_dir):
    m = re.fullmatch(r"jackson-datatype-([a-z0-9]+)-([\d.]+)\.jar", f)
    if m and m.group(2) != jackson_version:
        mod = f"jackson-datatype-{m.group(1)}"
        url = (
            "https://repo1.maven.org/maven2/com/fasterxml/jackson/datatype/"
            f"{mod}/{jackson_version}/{mod}-{jackson_version}.jar"
        )
        dest = os.path.join(jars_dir, f"{mod}-{jackson_version}.jar")
        urllib.request.urlretrieve(url, dest)  # noqa: S310 (fixed https Maven Central URL)
        os.remove(os.path.join(jars_dir, f))
        print(f"re-pinned {f} -> {mod}-{jackson_version}.jar")
