# Update

update的语法如下：

```sql
UPDATE table_name
SET column_name = expression [, ...]
[WHERE condition];

--例如
UPDATE employees
SET salary = salary * 1.1
WHERE department = 'Sales';
```

为了简单，condition部分我们仅先实现：`where column_name = xxx`

实现时，还是根据[基本架构](../01-BasicStructure)的思路，自顶向下实现语句。

Update的抽象语法树：

```
Update{
    table_name: String,
    columns: BTreeMap<String, Expression>,
    condition: Option<(String, Expression)>
}
```

### 代码实现