package fr.liglab.sami.utilities;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class BFInstantiator {
	public static BloomFilter<Integer> instantiateBF() {
		return BloomFilter.create(Funnels.integerFunnel(), 1000);
	}
}
