package com.mytt.ipdb

import java.io.FileWriter
import java.nio.charset.Charset
import java.nio.file.{Paths, Files}

class IP(val filename: String) {
	case class IpIndex(val ip: Long, val offset: Int, val len: Int)

	private var indexLength = 0
	private var firstIndexes: Array[Int] = _
	private var ipIndexes: Array[IpIndex] = _

	private var buffer: Array[Byte] = _

	def find(ip: String): Seq[String] = {
		val parts = ip.split("\\.")
		if (parts.length == 4) {
			val ipValue = (parts(0).toLong << 24) + (parts(1).toLong << 16) + (parts(2).toLong << 8) + parts(3).toLong

			var pos = firstIndexes((ipValue >>> 24).toInt)
			var found: IpIndex = null
			while (pos < ipIndexes.length && found == null) {
				val idx = ipIndexes(pos)

				if (idx.ip >= ipValue)
					found = idx

				pos += 1
			}

			if (found != null)
				readIp(found).split("\t")
			else
				Seq.empty
		} else
      Seq.empty
	}

	def load(): Unit = {
		/*
		1. file structure
		-----
		| 4 |													data offset(big endian)
		-----------
		| 1024		| 									first(head) index, 4 * 256
		----------------------
		| offset - 1024 - 4  |				ip index, 8 byte each index block
		----------------------
		| 1028 |											empty ???? why
		|------------------------
		| file_size - offset    |			ip info, data block
		-------------------------

		2. ip index
		--------------
		|     4      |								ip int(big endian)
		--------------
		|   3     |										data pos(offset) (little endian)
		-----------
		| 1 |													ip detail len
		----

		*/

		buffer = Files.readAllBytes(Paths.get(filename))
		indexLength = readLong(buffer, 0).toInt

		firstIndexes = new Array[Int](256)
		for (i <- 0 until firstIndexes.length)
			firstIndexes(i) = readLong(buffer, 4 + i * 4, false).toInt


		val offset = 1024 + 4

		ipIndexes = new Array[IpIndex]((indexLength - offset - 1024) / 8)

		for (i <- 0 until ipIndexes.length) {
			val ip = readLong(buffer, offset + i * 8)
			val posAndLen = readLong(buffer, offset + i * 8 + 4, false)
			ipIndexes(i) = new IpIndex(ip, (posAndLen & 0xFFFFFF).toInt, (posAndLen >>> 24).toInt)
		}
	}

	def export(fn: String): Unit = {
		val write = new FileWriter(fn)

		for (i <- 0 until ipIndexes.length) {
			val idx = ipIndexes(i)
			val next = if (i == ipIndexes.length - 1) 0xFFFFFFFFL else ipIndexes(i + 1).ip - 1
      write.write(s"${ipString(idx.ip)}, ${ipString(next)}, ${readIp(idx)}\n")
		}
		write.close()
	}

	def readIp(index: IpIndex): String = new String(buffer, index.offset + indexLength - 1024, index.len, Charset.forName("UTF-8"))

	def readLong(bytes: Array[Byte], idx: Int, bigEndian: Boolean = true, len: Int = 4): Long = {
		val seq = if (bigEndian) 0 until len else (len - 1).to(0, -1)

		var n = 0L
		for (i <- seq)
			n = (n << 8) + (bytes(idx + i) & 0xFF)
		n
	}

	def ipString(ip: Long) = s"${(ip >>> 24) & 0xFF}.${(ip >>> 16) & 0xFF}.${(ip >>> 8) & 0xFF}.${ip & 0xFF}"
}

object IP {
	def fromFile(fn: String): IP = {
		if (fn.endsWith(".dat"))
			new IP(fn)
    else if (fn.endsWith(".datx"))
			null
		else
			null
	}

	def main(args: Array[String]): Unit = {
		val db = fromFile("17monipdb.dat")

		db.load()

		println(db.find("118.26.233.247"))
		println(db.find("8.8.8.8"))

		db.export("ip_db.txt")
	}
}

