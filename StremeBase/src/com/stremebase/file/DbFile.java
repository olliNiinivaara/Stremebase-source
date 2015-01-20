/*
 * ---------------------------------------------------------
 * BEER-WARE LICENSED
 * This file is based on original work by Olli Niinivaara.
 * As long as you retain this notice you can do whatever
 * you want with this stuff. If you meet him one day, and
 * you think this stuff is worth it, you can buy him a
 * beer in return.
 * ---------------------------------------------------------
 */

package com.stremebase.file;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.stremebase.base.DB;


public class DbFile
{	
	public final long id;
	public final boolean persisted;
	public final String fileName;
	private ByteBuffer byteBuffer;
	private LongBuffer longBuffer;
	
	public long size;

	
	protected DbFile(long id, String fileName, long size, boolean persisted)
	{
		this.id = id;
		this.size = size;
		this.persisted = persisted;
		this.fileName = fileName;
	}
			
	protected void commit()
	{
		if (persisted && byteBuffer!=null) ((MappedByteBuffer)byteBuffer).force();
	}
	
	protected void delete()
	{
		longBuffer = null;
		byteBuffer = null;
		if (persisted) new File(fileName).delete();
	}
	
	public long read(long position)
	{
		if (byteBuffer == null) createBuffer();
		return longBuffer.get((int)position);
	}
	
	public void write(long position, long value)
	{
		if (byteBuffer == null) createBuffer();
		longBuffer.put((int)position, value);
	}
	
	public void write(int position, long[] array)
	{
		if (byteBuffer == null) createBuffer();
		longBuffer.position(position);
		longBuffer.put(array);
	}
	
	public void readToArray(int position, long[] array, int length)
	{
		if (byteBuffer == null) createBuffer();
		longBuffer.position(position);
		longBuffer.get(array, 0, length);
	}
	
	public long getCapacity()
	{
		if (byteBuffer == null) createBuffer();
		return longBuffer.capacity();
	}
			  	  
	protected void createBuffer()
	{		
		if (!persisted)
		{
			byteBuffer = ByteBuffer.allocate((int) (size*8));
			longBuffer = byteBuffer.asLongBuffer();
			return;
		}
					
		File file = new File(fileName);
				
		if (size==DB.NULL)
		{
			if (file.exists()) size = (int)file.length()/8;
			else return;
		}
						
		try (RandomAccessFile fileHandle = new RandomAccessFile(file.getAbsolutePath(), "rw"))
		{ 		  	
	  	FileChannel fileChannel = fileHandle.getChannel();
	  	byteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size*8);
	  	longBuffer = byteBuffer.asLongBuffer();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
