package main

import (
	"fmt"
	"log"

	"github.com/OpenStars/BackendService/StringBigsetService"
	"github.com/OpenStars/BackendService/StringBigsetService/bigset/thrift/gen-go/openstars/core/bigset/generic"
)

type MyObject struct {
	ID   int
	Name string
	Age  int
}

func (m MyObject) String() string {
	return fmt.Sprintf("ID: %d, Name: %s, Age: %d", m.ID, m.Name, m.Age)
}

func NewMyObject(id int, name string, age int) *MyObject {
	return &MyObject{
		ID:   id,
		Name: name,
		Age:  age,
	}
}

var bigset StringBigsetService.Client

func GetSlice(bskey string, startItem, count int32) ([]*generic.TItem, error) {
	return bigset.BsGetSlice(bskey, startItem, count)

}

func PutMultiItem(item []*generic.TBigsetItem) error {
	_, err := bigset.BsMultiPutBsItem(item)
	return err
}

func PutItem(bskey string, itemkey string, itemvalue string) (bool, error) {
	success, err := bigset.BsPutItem(bskey, itemkey, itemvalue)
	if err != nil {
		fmt.Println(err)
		return false, err
	}
	return success, nil
}

func GetItem(bskey, itemkey string) {
	item, err := bigset.BsGetItem(bskey, itemkey)
	if err != nil {
		log.Println("Error getting item:", err)
		return
	}
	fmt.Println("Item retrieved successfully:", string(item.Value))
}
func ListAllItem(){
    startIndex := int64(0)
	numItem := int32(1000)
	for {
		lsKey, err := bigset.GetListKey(startIndex, numItem)
		if err != nil || len(lsKey) == 0 {
			log.Fatalln("get list key", err)
		}
		fmt.Println("starIndext", startIndex, "totalKey", len(lsKey))
		for i, bskey := range lsKey {
			log.Println("bskey", bskey)
			totalItem, err := bigset.GetTotalCount(bskey)
			if err != nil {
				log.Fatalln(err)
			}
			startIndexItem := int32(0)
			totalRealItem := 0
			for startIndexItem < int32(totalItem) {
				lsItems, err := bigset.BsGetSlice(bskey, startIndexItem, numItem)
				if err != nil {
					log.Fatalln("item index", startIndexItem, err)
				}
				startIndexItem += numItem
				totalRealItem += len(lsItems)
			}
			if totalItem > 0 {
				log.Println(startIndex+int64(i), "bskey", bskey, "total Item", totalItem, "travel item", totalRealItem)
			}

		}
		startIndex += int64(numItem)
	}
}
func main() {
	bigset = StringBigsetService.NewClient(nil, "/test/dd2", "127.0.0.1", "20507")
    const bskey2="demo2"
	items := []*generic.TBigsetItem{
		{
			Bskey:     []byte(bskey2),
			Itemkey:   []byte("name"),
			Itemvalue: []byte("BÙi Minh Hiếu"),
		},
		{
			Bskey:     []byte(bskey2),
			Itemkey:   []byte("Name"),
			Itemvalue: []byte("Bui Anh Duc"),
		},
	}
	err := PutMultiItem(items)
	if err != nil {
		log.Fatalln("Failed to put multiple items:", err)
	}
	total, err := bigset.GetTotalCount(bskey2)
	if err != nil {
		log.Fatalln("Failed to get total count:", err)
	}

	lsItems, err := GetSlice(bskey2, 0, int32(total))
	if err != nil {
		log.Fatalln("Failed to get slice:", err)
	}

	for i, item := range lsItems {
		fmt.Printf("%d: Key = %s, Value = %s\n", i+1, string(item.Key), string(item.Value))
	}

	fmt.Println("Total items:", total)

}
