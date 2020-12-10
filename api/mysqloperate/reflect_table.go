package mysqloperate

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
	"github.com/urfave/cli"
)


var OriginColumn, TargetColumn string
//var ColumnsMap map[string]string

func ValueInList(value string, list []string) bool {
	for _, v := range list {
        if v==value{
        	return true
		}
	}
	return false
}

func MapGetKeys(m map[string]string) []string {
	var result []string
	for k := range m {
		result = append(result, k)
	}
	return result
}

func RowsColumns(rows *sql.Rows) ([]string, error) {
	return rows.Columns()
}

func RowsData(rows *sql.Rows, columns []string) ([]map[string]string) {
	//定义一个切片,长度是字段的个数,切片里面的元素类型是sql.RawBytes
	values := make([]sql.RawBytes, len(columns))
	//定义一个切片,元素类型是interface{} 接口
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		//把sql.RawBytes类型的地址存进去了
		scanArgs[i] = &values[i]
	}
	//获取字段值
	var result []map[string]string
	for rows.Next() {
		res := make(map[string]string)
		rows.Scan(scanArgs...)
		for i, col := range values {
			res[columns[i]] = string(col)
		}
		result = append(result, res)
	}
	return result
}

func QuerySql(db *sql.DB, querySql string) (*sql.Rows, error) {
	rows, err := db.Query(querySql)
	if err != nil {
		return nil, err
	}
	return rows, err
}

func TargetInsert(db *sql.DB, columnNameTarget []string, dataTarget []map[string]string, tableN string, defaultFieldValue map[string]string) {
	for _, v := range dataTarget {
		var insertValue []string
		for _, column := range columnNameTarget {
			if mapV, ok := v[column]; ok {
				insertValue = append(insertValue, mapV)
			}else if mapV, ok := defaultFieldValue[column]; ok {
				insertValue = append(insertValue, mapV)
			} else {
				insertValue = append(insertValue, "")
			}
		}
		insertSql := fmt.Sprintf("INSERT INTO %s(%s) VALUES ('%s')", tableN, strings.Join(columnNameTarget, ","),strings.Join(insertValue, "', '"))
		fmt.Println(insertSql)
		_, err := db.Exec(insertSql)
		if err != nil {
			fmt.Println("exec failed:", err, ", sql:", insertSql)
		}
	}
	fmt.Println("------数据插入成功；导入目标表过程结束------")

}


//给目标表的指定字段赋默认值
func TargetValueDefault(columnNameTarget []string) (map[string]string, error) {
	fmt.Println("是否需要对目标表指定字段赋值，需要请输入N,不需要请输入Y:")
	defaultFieldValue := make(map[string]string)
	var defaultValue string
	var err error
	fmt.Scanln(&defaultValue)
	if strings.ToLower(defaultValue)=="y"{
		err = errors.New("不需要赋值")
		return defaultFieldValue, err
	}else if strings.ToLower(defaultValue)=="n"{
		fmt.Println("请输入字段名及对应的值，分隔符为:。输入end结束")
		for {
			fmt.Scanln(&defaultValue)
			if strings.ToLower(defaultValue)=="end"{
				break
			}else{
				defaultValueList := strings.Split(defaultValue, ":")
				if len(defaultValueList)==2 && ValueInList(defaultValueList[0], columnNameTarget){
					defaultFieldValue[defaultValueList[0]] = defaultValueList[1]
				}else {
					fmt.Println("输入有误，请从失误地方重新输入")
				}
			}
		}
	}
	return defaultFieldValue, err
}

func MapReflect (dataOrigin []map[string]string, columnsMap map[string]string) ([]map[string]string) {
	var dataTarget []map[string]string
	var columnsMapKeys []string
	columnsMapKeys = MapGetKeys(columnsMap)
	for _, i := range dataOrigin {
		oneTargetData := make(map[string]string)
		oneOriginData := i
		for k,v := range oneOriginData {
			if ValueInList(k, columnsMapKeys){
				oneTargetData[columnsMap[k]] = v
			}
		}
		dataTarget = append(dataTarget, oneTargetData)
	}
	return dataTarget

}

func OrderSlice (listNeedOrder []string) ([]string, error) {
	fmt.Println("字段顺序是否无误，有误请输入N,无误请输入Y:")
	var columnsOrder string
	var err error
	var listOrder []string
	fmt.Scanln(&columnsOrder)
	if strings.ToLower(columnsOrder)=="y"{
		err = errors.New("输入无误")
		return listNeedOrder, err
	} else if strings.ToLower(columnsOrder)=="n"{
		fmt.Println("请按顺序输入,提前结束输入end返回未排序前字段顺序:")
		for {
			if len(listOrder)==len(listNeedOrder) {
				return listOrder, err
			}
			fmt.Scanln(&columnsOrder)
			if strings.ToLower(columnsOrder)=="end"{
				break
			}
			if ValueInList(columnsOrder, listNeedOrder)&& !ValueInList(columnsOrder, listOrder){
				listOrder = append(listOrder, columnsOrder)
			}else{
				fmt.Println("输入有误，请从失误地方重新输入")
			}
		}
	}
	return listNeedOrder, err

}

func DeleteSliceElement (listNeedDelete []string) ([]string, error) {
	fmt.Println("是否需要删除默认赋值的字段，需要请输入N,不需要请输入Y:")
	var columnsOrder string
	var err error
	var newList []string
	fmt.Scanln(&columnsOrder)
	if strings.ToLower(columnsOrder)=="y" {
		err = errors.New("不需要删除")
		return listNeedDelete, err
	} else if strings.ToLower(columnsOrder)=="n"{
		fmt.Println("请输入要删除的字段,用;隔开,中断操作输入end:")
		for {
			fmt.Scanln(&columnsOrder)
			if strings.ToLower(columnsOrder)=="end"{
				return listNeedDelete, err
			}else{
				deleteList := strings.Split(columnsOrder, ";")
				fmt.Println(deleteList)
				checkFlag := 0
				for _, v := range deleteList {
					if !ValueInList(v, listNeedDelete){
						checkFlag = 1
						break
					}
				}
				if checkFlag == 1{
					continue
				}
				for _, v := range listNeedDelete {
					if !ValueInList(v, deleteList) {
						newList = append(newList, v)
					}
				}
				return newList, err
			}
			fmt.Println("输入有误请重新输入")
		}
	}
	return newList, err
}

///原始表的列名和原始表的数据
func OriginColumnAndData (db *sql.DB, originTableName string) ([]string, []map[string]string) {
	//原始表
	dataOriginSql := fmt.Sprintf("select * from %s", originTableName)
	dataOriginRows, err := QuerySql(db, dataOriginSql)
	if err!=nil{
		fmt.Println("原始表查询有误，exit")
		os.Exit(0)
	}
	columnNameOrigin, _ := RowsColumns(dataOriginRows)
	fmt.Println(columnNameOrigin)
	columnNameOrigin, _ = OrderSlice(columnNameOrigin)
	fmt.Println("排序后的字段顺序", columnNameOrigin)
	dataOrigin := RowsData(dataOriginRows, columnNameOrigin)
	_ = dataOriginRows.Close()
	fmt.Printf("共获取%d行原始数据\n", len(dataOrigin))
	return columnNameOrigin, dataOrigin
}

//目标表的列名和目标表需要默认赋值的字段
func TargetColumnAndDefault (db *sql.DB, targetTableName string) ([]string, map[string]string) {
	//目标表
	dataTargetSql := fmt.Sprintf("select * from %s", targetTableName)
	dataTargetRows, err := QuerySql(db, dataTargetSql)
	if err!=nil{
		fmt.Println("目标表查询有误，exit")
		os.Exit(0)
	}
	columnNameTarget, _ := RowsColumns(dataTargetRows)
	fmt.Println(columnNameTarget)
	columnNameTarget, _ = OrderSlice(columnNameTarget)
	columnNameTarget, _ = DeleteSliceElement(columnNameTarget)
	fmt.Println("排序后的字段顺序", columnNameTarget)
	defaultFieldValue, _ := TargetValueDefault(columnNameTarget)
	fmt.Println("默认赋值:", defaultFieldValue)
	_ = dataTargetRows.Close()
	return columnNameTarget, defaultFieldValue
}


func OriginRelationTarget (columnNameOrigin []string, columnNameTarget []string) (map[string]string) {
	fmt.Println("请输入映射前后的字段空格隔开，OriginColumn TargetColumn，输入end结束: ")
	ColumnsMap := make(map[string]string)
	for {
		fmt.Scanln(&OriginColumn, &TargetColumn)
		if OriginColumn=="end"{
			break
		}
		if ValueInList(OriginColumn, columnNameOrigin) && ValueInList(TargetColumn, columnNameTarget) {
			//将目标字段和原始字段建立映射
			ColumnsMap[OriginColumn] = TargetColumn
		}else{
			fmt.Println("输入有误，请从失误地方重新输入")
		}
		fmt.Println("请继续输入:")
	}
	fmt.Println(ColumnsMap)
	return ColumnsMap
}

func ReflectProcess(dbOrigin *sql.DB, dbTarget *sql.DB , originTableName string, targetTableName string) {

	//原始表的列名和原始表的数据
	columnNameOrigin, dataOrigin := OriginColumnAndData(dbOrigin, originTableName)
	//目标表的列名和目标表需要默认赋值的字段
	columnNameTarget, defaultFieldValue := TargetColumnAndDefault(dbTarget, targetTableName)
	//原始表和目标表建立字段关系
	ColumnsMap := OriginRelationTarget(columnNameOrigin, columnNameTarget)
	//输出数据至目标表
	dataTarget := MapReflect(dataOrigin, ColumnsMap)
	fmt.Printf("共映射%d行目标数据", len(dataTarget))
	//将数据加入目标表中
	TargetInsert(dbTarget, columnNameTarget, dataTarget, targetTableName, defaultFieldValue)
	//关闭DB
	dbOrigin.Close()
}


//将MYSQL数据库原始表数据指定映射字段导入新的目标表
func ReflectTwoSqlTable(c *cli.Context) {
	DbFlag := c.String("s")
	if strings.ToLower(DbFlag)=="y"{
		originDbHost := c.String("oh")
		originPort := c.String("op")
		originPassword := c.String("opw")
		originDbName := c.String("odn")
		originTableName := c.String("ot")
		targetTableName := c.String("tt")
		dataSourceName := fmt.Sprintf("root:%s@tcp(%s:%s)/%s?charset=utf8", originPassword, originDbHost, originPort, originDbName)
		dbOrigin, err := sql.Open("mysql", dataSourceName)
		if err!=nil {
			fmt.Println(err)
		}
		ReflectProcess(dbOrigin, dbOrigin, originTableName, targetTableName)
	}else {
		//原始表的数据库相关信息
		originDbHost := c.String("oh")
		originPort := c.String("op")
		originPassword := c.String("opw")
		originDbName := c.String("odn")
		originTableName := c.String("ot")
		//目标表的数据库的相关信息
		targetDbHost := c.String("th")
		targetPort := c.String("tp")
		targetPassword := c.String("tpw")
		targetDbName := c.String("tdn")
		targetTableName := c.String("tt")
		dataSourceName := fmt.Sprintf("root:%s@tcp(%s:%s)/%s?charset=utf8", originPassword, originDbHost, originPort, originDbName)
		dbOrigin, err := sql.Open("mysql", dataSourceName)
		if err!=nil {
			fmt.Println(err)
		}
		dataSourceName = fmt.Sprintf("root:%s@tcp(%s:%s)/%s?charset=utf8", targetPassword, targetDbHost, targetPort, targetDbName)
		dbTarget, err := sql.Open("mysql", dataSourceName)
		if err!=nil {
			fmt.Println(err)
		}
		ReflectProcess(dbOrigin, dbTarget, originTableName, targetTableName)
	}

}




