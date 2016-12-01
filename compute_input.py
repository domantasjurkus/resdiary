import sys
import json
import numpy as np


def read_in():
	lines = sys.stdin.readlines()
	return json.loads(lines[0])


def main():
	lines = read_in()
	
	np_lines = np.array(lines)

	lines_sum = np.sum(np_lines)

	print lines_sum

	temp = {}
	temp[1] = "A"
	temp[2] = "B"

	print temp.keys()
	print temp.values()

if __name__ == "__main__":
	main()
