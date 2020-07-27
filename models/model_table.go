package models

// 定义产品表
type Product struct {
	Code string
	Price uint
}

//定义表
type Admin struct {
	Name    string
	Product string
	Address string
}
