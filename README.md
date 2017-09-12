2017-09-12

    1.RDDFromJavaList 使用Java List 集合数据构造 Spark RDD 对象，并操作
    2.本来打算使用当前版本 spark-core_2.11 2.2.0,结果报告异常 ：
        java.lang.ClassFormatError: Illegal UTF8 string in constant pool in class file org/apache/spark/scheduler/TaskSetManager
        目前暂时没有针对此异常的解决方法，先退回到 spark-core_2.11 2.0.2


