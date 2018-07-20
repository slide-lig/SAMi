package fr.liglab.sami.utilities;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

import net.openhft.koloboke.collect.Equivalence;
import net.openhft.koloboke.collect.ObjCollection;
import net.openhft.koloboke.collect.map.ObjObjCursor;
import net.openhft.koloboke.collect.map.ObjObjMap;
import net.openhft.koloboke.collect.set.ObjSet;

public class ArrayMap<K, V> implements ObjObjMap<K, V> {
	private Object[] storage;
	private int nbInserted = 0;

	public ArrayMap(int maxSize) {
		this.storage = new Object[maxSize * 2];
	}

	@Override
	public int size() {
		return nbInserted;
	}

	@Override
	public boolean isEmpty() {
		return nbInserted == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		for (int i = 0; i < this.nbInserted * 2; i += 2) {
			if (this.storage[i].equals(key)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		for (int i = 1; i < this.nbInserted * 2; i += 2) {
			if (this.storage[i].equals(value)) {
				return true;
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		for (int i = 0; i < this.nbInserted * 2; i += 2) {
			if (this.storage[i].equals(key)) {
				return (V) storage[i + 1];
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V put(K key, V value) {
		int i = 0;
		for (; i < this.nbInserted * 2; i += 2) {
			if (this.storage[i].equals(key)) {
				Object prev = storage[i + 1];
				storage[i + 1] = value;
				return (V) prev;
			}
		}
		this.storage[i] = key;
		this.storage[i + 1] = value;
		this.nbInserted++;
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		for (int i = 0; i < this.nbInserted * 2; i += 2) {
			if (this.storage[i].equals(key)) {
				Object prev = this.storage[i + 1];
				this.remove(i);
				return (V) prev;
			}
		}
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		Arrays.fill(this.storage, null);
		this.nbInserted = 0;
	}

	@Override
	public long sizeAsLong() {
		return this.nbInserted;
	}

	@Override
	public boolean ensureCapacity(long minSize) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean shrink() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Equivalence<K> keyEquivalence() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Equivalence<V> valueEquivalence() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean forEachWhile(BiPredicate<? super K, ? super V> predicate) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ObjObjCursor<K, V> cursor() {
		return new Cursor();
	}

	@Override
	public ObjSet<K> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ObjCollection<V> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ObjSet<java.util.Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeIf(BiPredicate<? super K, ? super V> filter) {
		throw new UnsupportedOperationException();
	}

	private void remove(int pos) {
		if (pos == (this.nbInserted - 1) * 2) {
			storage[pos] = null;
			storage[pos + 1] = null;
		} else {
			int lastInsert = (this.nbInserted - 1) * 2;
			storage[pos] = storage[lastInsert];
			storage[pos + 1] = storage[lastInsert + 1];
			storage[lastInsert] = null;
			storage[lastInsert + 1] = null;
		}
		this.nbInserted--;
	}

	private class Cursor implements ObjObjCursor<K, V> {

		private int pos = -1;
		private boolean hasRemoved = false;

		@Override
		public boolean moveNext() {
			if (hasRemoved) {
				hasRemoved = false;
			} else {
				pos += 2;
			}
			return pos < storage.length;
		}

		@Override
		public void remove() {
			ArrayMap.this.remove(pos);
		}

		@Override
		public void forEachForward(BiConsumer<? super K, ? super V> action) {
			throw new UnsupportedOperationException();
		}

		@SuppressWarnings("unchecked")
		@Override
		public K key() {
			return (K) storage[pos];
		}

		@SuppressWarnings("unchecked")
		@Override
		public V value() {
			return (V) storage[pos + 1];
		}

		@Override
		public void setValue(V value) {
			storage[pos + 1] = value;
		}

	}

}
