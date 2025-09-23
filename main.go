package main

import "fmt"

/**
 * 代码中的类名、方法名、参数名已经指定，请勿修改，直接返回方法规定的值即可
 *
 *
 * @param cost int整型一维数组
 * @return int整型
 */
func minCostClimbingStairs(cost []int) int {
	// write code here

	memory := make(map[int]int)
	memory[0] = 0
	memory[1] = 0
	memory[len(cost)-1] = cost[len(cost)-1]
	memory[len(cost)-2] = cost[len(cost)-2]
	for i := len(cost) - 3; i >= 0; i-- {
		memory[i] = min(memory[i+1]+cost[i], memory[i+2]+cost[i])
	}
	// s[l] = min{s[l+1] + c[l], s[l+2] + c[l]}
	// 其中 0 < l < n-1
	// s[0] s[1]
	return min(memory[0], memory[1])
}

func main() {

	fmt.Println(minCostClimbingStairs([]int{2, 5, 20}))
}
