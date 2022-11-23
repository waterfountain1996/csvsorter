package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"log"
	"path/filepath"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

type Record []string						// type alias for csv record
type Visitor func(*Node)					// type alias for visitor function for tree traversal
type Comparator func(Record, Record) bool	// type alias for function used to compare two records

type Tree struct {
	sortIndex uint
	root *Node
} 

type Node struct {
	val Record
	left *Node
	right *Node
}

func NewTree(sortIndex uint) *Tree {
	return &Tree{sortIndex: sortIndex, root: nil}
}

func (t *Tree) Insert(value Record) {
	if t.root == nil {
		t.root = &Node{val: value}
	} else {
		if t.sortIndex >= uint(len(value)) {
			log.Fatalf("index out of range\n")
		}
		if len(value) != len(t.root.val) {
			log.Fatalf("invalid record length: %d\n", len(value))
		}
		t.root.insert(value, func(left Record, right Record) bool {
			return left[t.sortIndex] < right[t.sortIndex]
		})
	}
}

func (t *Tree) Traverse(reverse bool, visit Visitor) {
	if t.root != nil {
		traverseInOrder(t.root, reverse, visit)
	}
}

func (n *Node) insert(value Record, cmp Comparator) {
	if cmp(value, n.val) {
		if n.left == nil {
			n.left = &Node{val: value}
		} else {
			n.left.insert(value, cmp)
		}
	} else {
		if n.right == nil {
			n.right = &Node{val: value}
		} else {
			n.right.insert(value, cmp)
		}
	}
}

func traverseInOrder(node *Node, reverse bool, visit Visitor) {
	if node == nil {
		return
	}

	var first, second *Node
	if reverse {
		first, second = node.right, node.left
	} else {
		first, second = node.left, node.right
	}

	traverseInOrder(first, reverse, visit)
	visit(node)
	traverseInOrder(second, reverse, visit)
}

func BuildTreeFromStream(tree *Tree, stream <-chan Record) {
	for record := range stream {
		tree.Insert(record)
	}
}

func WriteTree(tree *Tree, writer *csv.Writer, reverse bool) {
	tree.Traverse(reverse, func(node *Node) {
		writer.Write(node.val)
	})
	writer.Flush()
}

func ReadCSVFromFile(filename string, stream chan<- Record, wg *sync.WaitGroup) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			break
		}

		value := strings.Split(line, ",")
		stream <- value
	}

	wg.Done()
}

func ReadIncomingFiles(fileStream <-chan string, recordStream chan<- Record, wg *sync.WaitGroup) {
	for filename := range fileStream {
		wg.Add(1)
		go ReadCSVFromFile(filename, recordStream, wg)
	}
	wg.Done()
}

func main() {
	var tree *Tree						// binary sort tree
	var writer *csv.Writer				// result writer

	var ifname, ofname, dirname string	// input file, output file, input directory
	var index uint						// csv record index to sort by
	var reverse, skipHeader bool		// reverse output, omit header sorting

	var wg sync.WaitGroup				// concurrent reader synchronization
	var fileStream chan string			// csv file stream
	var recordStream chan Record		// csv record stream
	var backgroundTask func()			// goroutine for listing directory contents

	flag.StringVar(&ifname, "i", "", "Input CSV file")
	flag.StringVar(&ofname, "o", "", "Output CSV file")
	flag.StringVar(&dirname, "d", "", "Input directory")
	flag.UintVar(&index, "f", 1, "Sort records by Nth field")
	flag.BoolVar(&reverse, "r", false, "Sort records in reverse order")
	flag.BoolVar(&skipHeader, "h", false, "Ignore header when sorting")

	flag.Parse()
	
	tree = NewTree(index - 1)

	if ofname != "" {
		f, err := os.Create(ofname)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		writer = csv.NewWriter(f)
	} else {
		writer = csv.NewWriter(os.Stdout)
	}

	if dirname != "" && ifname != "" {
		log.Fatal("can't use -d and -i flags at once")
	}

	fileStream = make(chan string)
	recordStream = make(chan Record)

	// launch tree builder goroutine
	wg.Add(1)
	go BuildTreeFromStream(tree, recordStream)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)
	go func() {
		<- sigChan
		WriteTree(tree, writer, reverse)
		os.Exit(0)
	}()

	// launch file reader goroutine
	wg.Add(1)
	go ReadIncomingFiles(fileStream, recordStream, &wg)

	if dirname != "" {
		// walk through directory and send each .csv file to stream
		backgroundTask = func() {
			err := filepath.Walk(dirname, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if strings.HasSuffix(info.Name(), ".csv") {
					fileStream <- path
				}
				return nil
			})
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		}
	} else {
		// use stdin if no input file is provided
		if ifname == "" {
			ifname = os.Stdin.Name()
		}
		// send a single file to stream
		backgroundTask = func() {
			fileStream <- ifname
			wg.Done()
		}
	}

	wg.Add(1)
	go backgroundTask()

	wg.Done()	// ReadIncomingFiles
	wg.Done()	// BuildTreeFromStream
	wg.Wait()

	WriteTree(tree, writer, reverse)
}
