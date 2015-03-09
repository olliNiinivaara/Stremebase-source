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
 
package com.stremebase.base.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;

import com.stremebase.base.DB;


public class Cache
{
  public interface CacheDropObserver
  {
    boolean notify(long key, Object dropped);
  }

  protected CacheDropObserver cacheDropObserver;
  
  protected static final Collection<Cache> caches = new ArrayList<>();
  
  private final Map<Object, SoftValue> map = new HashMap<Object, SoftValue>(DB.db.MAXCACHEDSETSIZE/4);
  private final ReferenceQueue<SoftValue> queue = new ReferenceQueue<SoftValue>();

  
  public Cache(CacheDropObserver cacheDropObserver)
  {
    this.cacheDropObserver = cacheDropObserver;
    caches.add(this);
  }
  
  public void clear()
  {
    map.clear();
  }
  
  public Object get(final long key)
  {
    for (Cache cache: caches) cache.processQueue();
    final SoftValue reference = map.get(key);
    if (reference==null) return null;
    return reference.get();
  }

  public void put(final long key, final Object object)
  {
    for (Cache cache: caches) cache.processQueue();
    map.put(key, new SoftValue(object, key, queue));
  }

  public void remove(final long key)
  {
    map.remove(key);
  }
  
  public void notifyForKey(final long key)
  {
    if (cacheDropObserver == null) return;
    SoftValue sv = map.get(key);
    if (sv!=null) cacheDropObserver.notify(sv.key, sv.get());
  }
  
  public void notifyForAll()
  {
    if (cacheDropObserver == null) return;
    
    Iterator<SoftValue> iter = map.values().iterator();
    while (iter.hasNext())
    {
      SoftValue sv = iter.next();
      boolean remove = cacheDropObserver.notify(sv.key, sv.get());
      if (remove) iter.remove();
    }
    
    //for (SoftValue sv: map.values()) cacheDropObserver.notify(sv.key, sv.get());
  }
  
  private void processQueue()
  {
    SoftValue sv;
    while ((sv = (SoftValue)queue.poll()) != null)
    {
      if (cacheDropObserver!=null) cacheDropObserver.notify(sv.key, sv.get());
      map.remove(sv.key);
    }
  }

  @SuppressWarnings("rawtypes")
  private static class SoftValue extends SoftReference
  {
    private final long key; 

    @SuppressWarnings("unchecked")
    private SoftValue(Object object, long key, ReferenceQueue<SoftValue> queue)
    {
      super(object, queue);
      this.key = key;
    }
  }
}
