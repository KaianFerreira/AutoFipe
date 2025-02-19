<template>
  <div class="fixed lg:relative bottom-0 left-0 right-0 lg:bottom-auto bg-white dark:bg-gray-800 shadow-lg lg:shadow rounded-t-xl lg:rounded-xl">
    <!-- Search Bar -->
    <div class="p-4 border-b dark:border-gray-700">
      <div class="relative">
        <input
          type="text"
          v-model="searchQuery"
          placeholder="Search vehicles..."
          class="w-full pl-10 pr-4 py-2 rounded-lg border dark:border-gray-600 dark:bg-gray-700 dark:text-white focus:ring-2 focus:ring-indigo-500 dark:focus:ring-indigo-400"
        />
        <svg
          class="absolute left-3 top-2.5 w-5 h-5 text-gray-400"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            stroke-linecap="round"
            stroke-linejoin="round"
            stroke-width="2"
            d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
          />
        </svg>
      </div>
    </div>

    <!-- Filters Toggle Button (Mobile) -->
    <div class="lg:hidden border-b dark:border-gray-700">
      <button
        @click="isFilterOpen = !isFilterOpen"
        class="flex items-center justify-between w-full p-4 text-left"
      >
        <span class="font-medium text-gray-900 dark:text-white">Filters</span>
        <svg
          class="w-5 h-5 transform transition-transform"
          :class="{ 'rotate-180': isFilterOpen }"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
        </svg>
      </button>
    </div>

    <!-- Filters Content -->
    <div
      class="overflow-hidden transition-all duration-300"
      :class="[
        isFilterOpen || (isLargeScreen && mounted) ? 'max-h-[80vh] lg:max-h-none' : 'max-h-0',
        'lg:block'
      ]"
    >
      <div class="p-4 space-y-4">
        <div>
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Brand</label>
          <select
            v-model="filters.brand"
            class="w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white"
          >
            <option :value="null">All Brands</option>
            <option v-for="brand in brands" :key="brand.codigo" :value="brand.codigo">
              {{ brand.nome }}
            </option>
          </select>
        </div>

        <div>
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Model</label>
          <select
            v-model="filters.model"
            class="w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white"
          >
            <option :value="null">All Models</option>
            <option v-for="model in filteredModels" :key="model.codigo" :value="model.codigo">
              {{ model.nome }}
            </option>
          </select>
        </div>

        <div>
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Year</label>
          <select
            v-model="filters.year"
            class="w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white"
          >
            <option :value="null">All Years</option>
            <option v-for="year in availableYears" :key="year" :value="year">
              {{ year }}
            </option>
          </select>
        </div>

        <div>
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Price Range</label>
          <div class="grid grid-cols-2 gap-2">
            <div>
              <input
                type="number"
                v-model.number="filters.priceRange[0]"
                :min="priceRange.min"
                :max="filters.priceRange[1]"
                placeholder="Min"
                class="w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white"
              />
            </div>
            <div>
              <input
                type="number"
                v-model.number="filters.priceRange[1]"
                :min="filters.priceRange[0]"
                :max="priceRange.max"
                placeholder="Max"
                class="w-full rounded-md border-gray-300 dark:border-gray-600 dark:bg-gray-700 dark:text-white"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { storeToRefs } from 'pinia'
import { useVehicleStore } from '~/stores/vehicles'
import { useMediaQuery } from '@vueuse/core'

const store = useVehicleStore()
const { brands, models, filters, availableYears, priceRange } = storeToRefs(store)

const isFilterOpen = ref(false)
const mounted = ref(false)
const isLargeScreen = useMediaQuery('(min-width: 1024px)')
const searchQuery = ref('')

onMounted(() => {
  mounted.value = true
})

const filteredModels = computed(() => {
  if (!filters.value.brand) return models.value
  return models.value.filter(model => model.marca_id === filters.value.brand)
})

// Watch search query and update store filters
watch(searchQuery, (newQuery) => {
  store.setSearchQuery(newQuery)
})
</script>