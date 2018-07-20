package fr.liglab.sami.utilities;

import java.util.Map;
import java.util.function.BiConsumer;

import fi.tkk.ics.jbliss.Reporter;
import fr.liglab.sami.code.DFSCode;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

public class AutomorphReporter implements Reporter {
	public AutomorphReporter(DFSCode code) {
		this.code = code;
		this.smallestMap = new int[code.getNbNodes()];
		for (int i = 0; i < smallestMap.length; i++) {
			smallestMap[i] = -1;
		}
		this.buffer = new IntSet[code.getNbNodes()];
		for (int i = 0; i < this.buffer.length; i++) {
			this.buffer[i] = HashIntSets.newMutableSet(this.buffer.length);
			this.buffer[i].add(i);
		}
		if (code.requiresAutomorphismVerif()) {
			this.automorphVerif = new int[code.getNbNodes()];
		}
	}

	public int[] getMappings() {
		for (int i = 0; i < this.buffer.length; i++) {
			if (this.smallestMap[i] == -1) {
				IntCursor c = this.buffer[i].cursor();
				int min = Integer.MAX_VALUE;
				while (c.moveNext()) {
					min = Math.min(min, c.elem());
				}
				c = this.buffer[i].cursor();
				while (c.moveNext()) {
					this.smallestMap[c.elem()] = min;
				}
			}
		}
		return this.smallestMap;
	}

	// public boolean isValidMapping() {
	// return true;
	// }
	private DFSCode code;
	private int[] smallestMap;
	private int[] automorphVerif;
	private IntSet[] buffer;
	// private IntSet[] mappings;
	@SuppressWarnings({ "rawtypes" })
	private BiConsumer biConsumer = new BiConsumer() {

		@Override
		public void accept(Object k, Object v) {
			int ki = ((Integer) k).intValue();
			int vi = ((Integer) v).intValue();
			if (buffer[vi] != buffer[ki]) {
				buffer[ki].addAll(buffer[vi]);
				IntCursor c = buffer[ki].cursor();
				while (c.moveNext()) {
					buffer[c.elem()] = buffer[ki];
				}
			}
		}
	};
	@SuppressWarnings({ "rawtypes" })
	private BiConsumer fillArray = new BiConsumer() {

		@Override
		public void accept(Object k, Object v) {
			int ki = ((Integer) k).intValue();
			int vi = ((Integer) v).intValue();
			automorphVerif[ki] = vi;
		}
	};

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void report(Map m, Object p) {
		if (automorphVerif != null) {
			m.forEach(fillArray);
			if (this.code.checkValidAutomorphism(this.automorphVerif)) {
				m.forEach(biConsumer);
			}
		} else {
			m.forEach(biConsumer);
		}
	}

}
