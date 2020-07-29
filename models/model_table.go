package models

// 定义产品表
type Product struct {
	Code string
	Price uint
}

// 定义表
type Admin struct {
	Name    string
	Product string
	Address string
}

// 定义本地消息表
type LocalMsg struct {
	ID      string
	Message string
	State   bool
}