#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <limits.h>
#include <thread>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <sstream>

#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;
using namespace std::chrono;

// variables
//  Define a typedef for time point
typedef time_point<system_clock> TimePoint;
// Define a structure to track page accesses within the time window for each process
unordered_map<int, vector<int>> workingSetMap; // Map to store page accesses within the time window for each process
unordered_map<int, int> pageFaultCounts; // Map to store page fault counts for each process
int totalPageFaults = 0; // Total number of page faults
const int WORKING_SET_WINDOW = 1000; // 1 second


stack<int> pageAccessStack;

// Define Page structure
struct Page {
  int frameID;
  bool isAllocated;
  // Add any additional fields if needed
};

// Page table entry
struct PageTableEntry {
  int frameID;
  bool isAllocated;
  int pageID;

  // Add more fields as needed
};

// Define Frame structure
struct Frame {
  int frameID;
  int processID;
  int pageID;
  int accessTime;
  int pageNumber;
  bool isAllocated;
  // Add any additional fields if needed
};

// Frame table entry
struct FrameTableEntry {
  int processID;
  int pageID;
  bool isAllocated;
  // Add more fields as needed
};

// Disk page table entry
struct DiskPageTableEntry {
  int processID;
  int pageID;
  // Add more fields as needed
};

// Define Process structure
struct Process {
  int processID;
  int totalPageFramesOnDisk;
  vector<PageTableEntry> pageTable;
  // Add any additional fields if needed
};

// Define global variables
vector<Process> processes;
vector<Frame> framesInMainMemory;
vector<DiskPageTableEntry> diskPageTable;
int totalPageFrames = 0;
int totalProcesses = 0;
int pageFramePerProcess = 0;

#pragma region  -------- protytpe functions


void updatePageTable(int processID, int pageNumber, bool isAllocated);
void updateWorkingSet(int processID, int pageNumber);
int calculateWorkingSetSize(int processID);
void displayStatistics();
void handleDiskIORequest();
void *diskDriver(void *arg);
void pageAccessHandler();
int extractPageNumber(string address);
void readInputFromFile(const string &filename);
void initializeSemaphores();
int findFreeFrame();
void displayStatisticsPeriodically();
void *pageFaultHandler(void *arg);
void *pageFaultHandlerWS(void *arg);
void replacePageLIFO(int processID, int pageNumber);
void replacePageMRU(int processID, int pageNumber);
void replacePageLRU(int processID, int pageNumber);
void replacePageWS(int processID, int pageNumber);
void replacePageOPT_X(int processID, int pageNumber, int lookaheadWindowSize);
void replacePageLRU_X(int processID, int pageNumber, int lookaheadWindowSize);


#pragma endregion 


// Define semaphores
sem_t pageFaultSem;
sem_t diskDriverSem;

// Function to get the current time in milliseconds
long long getCurrentTime() {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void initializeDataStructures() {
    // Initialize processes
    processes.resize(totalProcesses);
    for (int i = 0; i < totalProcesses; ++i) {
        Process process;
        process.processID = i;
        process.totalPageFramesOnDisk = pageFramePerProcess;
        process.pageTable.resize(pageFramePerProcess);
        for (int j = 0; j < pageFramePerProcess; ++j) {
            PageTableEntry entry;
            entry.frameID = -1; // Not allocated
            entry.isAllocated = false;
            entry.pageID = j;
            process.pageTable[j] = entry;
        }
        processes[i] = process;
    }

    // Initialize frames in main memory
    framesInMainMemory.resize(totalPageFrames);
    for (int i = 0; i < totalPageFrames; ++i) {
        Frame frame;
        frame.frameID = i;
        frame.processID = -1; // No process allocated
        frame.pageID = -1; // No page allocated
        frame.accessTime = 0;
        frame.isAllocated = false;
        framesInMainMemory[i] = frame;
    }

    // Initialize disk page table (if needed)
    // diskPageTable.resize(...);
    // Initialize other data structures as needed
}

// Function to update the page table entry
void updatePageTable(int processID, int pageNumber, bool isAllocated) {
  for (auto &entry : processes[processID].pageTable) {
    if (entry.pageID == pageNumber) {
      entry.isAllocated = isAllocated;
      break;
    }
  }
}

// Function to update the Working Set for a process
void updateWorkingSet(int processID, int pageNumber) {
    // Check if the process has any previous page accesses within the time window
    if (workingSetMap.find(processID) == workingSetMap.end()) {
        // If not, create a new entry for the process
        workingSetMap[processID] = {pageNumber};
    } else {
        // If yes, add the current page access to the existing entries
        workingSetMap[processID].push_back(pageNumber);
    }
    // Remove page accesses that are older than the time window
    while (!workingSetMap[processID].empty() && getCurrentTime() - workingSetMap[processID].front() > WORKING_SET_WINDOW) {
        workingSetMap[processID].erase(workingSetMap[processID].begin());
    }
}

// Function to calculate the size of the Working Set for a process
int calculateWorkingSetSize(int processID) {
    return workingSetMap[processID].size();
}

// Function to display statistics, including the minimum and maximum size of the Working Set
void displayStatistics() {
    // Display the number of page faults for each process
    cout << "Number of page faults for each process:" << endl;
    for (const auto &entry : pageFaultCounts) {
        cout << "Process " << entry.first << ": " << entry.second << " page faults" << endl;
    }
    // Calculate and display the total number of page faults
    cout << "Total number of page faults: " << totalPageFaults << endl;
    // Display the minimum and maximum size of the Working Set for each process
    cout << "Minimum and maximum size of the Working Set for each process:" << endl;
    for (const auto &entry : workingSetMap) {
        int minSize = INT_MAX;
        int maxSize = 0;
        for (int pageNumber : entry.second) {
            minSize = min(minSize, pageNumber);
            maxSize = max(maxSize, pageNumber);
        }
        cout << "Process " << entry.first << ": Min size = " << minSize << ", Max size = " << maxSize << endl;
    }
}


// Page fault handler function
void *pageFaultHandlerMRU(void *arg) {
  int processID = 0;
  int pageNumber = 0;

  while (true) {
    // Wait for page fault signal
    sem_wait(&pageFaultSem);

    // Handle page fault using MRU algorithm
    replacePageMRU( processID ,  pageNumber );

    // Signal disk driver semaphore
    sem_post(&diskDriverSem);
  }
  return NULL;
}

// Page fault handler function
void *pageFaultHandlerLIFO(void *arg) {
  int processID = 0;
  int pageNumber = 0;

  while (true) {
    // Wait for page fault signal
    sem_wait(&pageFaultSem);

    // Handle page fault using LIFO algorithm
    replacePageLIFO( processID ,  pageNumber );

    // Signal disk driver semaphore
    sem_post(&diskDriverSem);
  }
  return NULL;
}

void *pageFaultHandlerLRU_X(void *arg) {
    int processID = 0;
    int pageNumber = 0;
    int lookaheadWindowSize = *(int*)arg;

    while (true) {   
        // Wait for page fault signal
        sem_wait(&pageFaultSem);

        // Handle page fault using LRU-X algorithm
        replacePageLRU_X(processID, pageNumber, lookaheadWindowSize);

        // Signal disk driver semaphore
        sem_post(&diskDriverSem);
    }
    return NULL;
}

void *pageFaultHandlerWS(void *arg) {
  int processID = 0;
  int pageNumber = 0;

  while (true) {
    // Wait for page fault signal
    sem_wait(&pageFaultSem);

    // Handle page fault using WS algorithm
    replacePageWS( processID ,  pageNumber );

    // Signal disk driver semaphore
    sem_post(&diskDriverSem);
  }
  return NULL;
}

// Function to handle page replacement using OPT-X algorithm with the specified lookahead window size


void handleDiskIORequest() {
    // Simulate disk I/O requests
    // For example, you can generate a random number to represent the probability of a disk I/O request occurring
    int diskIOProbability = rand() % 100; // Generate a random number between 0 and 99
    if (diskIOProbability < 20) {
        // Simulate a disk I/O request with a probability of 20%
        // Implement the logic to handle the disk I/O request
        // This might involve updating data structures, simulating disk access times, etc.

        // For example, you can simulate a disk access time by sleeping for a certain duration
        std::this_thread::sleep_for(std::chrono::milliseconds(50)); // Simulate a disk access time of 50 milliseconds
    }
}


// Disk driver function
void *diskDriver(void *arg) {
  while (true) {
    // Wait for disk I/O request
    sem_wait(&diskDriverSem);

    // Handle disk I/O request
    handleDiskIORequest();

    // Signal page fault semaphore
    sem_post(&pageFaultSem);
  }
  return NULL;
}

// Function to extract page number from address
int extractPageNumber(string address) {
  size_t pos = address.find_first_of(' ');
  if (pos != string::npos) {
    string pageNumberStr = address.substr(pos + 1);
    return stoi(pageNumberStr, nullptr, 0);
  }
  return -1; // Invalid page number
}

void readInputFromFile(const string &filename) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Error: Unable to open file " << filename << endl;
        exit(1);
    }

    // Variables to track whether required information is read from the file
    bool tpRead = false;  // Total page frames
    bool psRead = false;  // Page frames per process
    bool kRead = false;   // Total processes

    string line;
    while (getline(file, line)) {
        istringstream iss(line);
        string token;
        iss >> token;

        if (token == "tp") {
            // Read total page frames
            iss >> totalPageFrames;
            tpRead = true;
        } else if (token == "ps") {
            // Read page frames per process
            iss >> pageFramePerProcess;
            psRead = true;
        } else if (token == "k") {
            // Read total processes
            iss >> totalProcesses;
            kRead = true;
            processes.resize(totalProcesses);
            // Initialize processes...
        } else if (isdigit(token[0])) { // Process id and address pairs
            // Validate and process address...
        }
    }

    file.close();

    // Check if all required information is read from the file
    if (!tpRead || !psRead || !kRead) {
        cerr << "Error: Input file does not contain required information." << endl;
        exit(1);
    }
}

// Initialize semaphores
void initializeSemaphores() {
  sem_init(&pageFaultSem, 0, 0);
  sem_init(&diskDriverSem, 0, 0);
}

// Function to find the least recently used page in the frame table
int findLRUPage() {
  // int INT_MAX = 0;
  int minAccessTime = INT_MAX;
  int lruPage = -1;
  for (const auto &frame : framesInMainMemory) {
    if (frame.isAllocated && frame.accessTime < minAccessTime) {
      minAccessTime = frame.accessTime;
      lruPage = frame.pageID;
    }
  }
  return lruPage;
}

// Function to handle page accesses
void pageAccessHandler() {
    // Perform page access simulation logic
    // This could involve updating page access statistics, triggering page faults, etc.

    // For each page access, update the Working Set and handle page faults
    for (const auto &process : processes) {
        // Simulate page accesses for each process (assuming for demonstration)
        int pageNumber = rand() % process.totalPageFramesOnDisk; // Generate a random page number within the process's range
        // Update the Working Set for the current process
        updateWorkingSet(process.processID, pageNumber);
        // Check if the page is allocated in main memory
        bool pageAllocated = false;
        for (const auto &frame : framesInMainMemory) {
            if (frame.processID == process.processID && frame.pageID == pageNumber) {
                pageAllocated = true;
                break;
            }
        }
        // If the page is not allocated in main memory, trigger a page fault
        if (!pageAllocated) {
            // Increment page fault count for the current process
            pageFaultCounts[process.processID]++;
            // Signal page fault semaphore
            sem_post(&pageFaultSem);
        }
    }
    // Calculate the size of the working set periodically
  // Calculate the size of the working set periodically
  if (getCurrentTime() % WORKING_SET_WINDOW == 0) {
      // Display the working set size for each process
      for (const auto &process : processes) {
          int workingSetSize = calculateWorkingSetSize(process.processID);
          cout << "Working Set Size for Process " << process.processID << ": " << workingSetSize << endl;
      }
  }
}


int findFreeFrame();

// Function to handle page replacement using LIFO algorithm
void replacePageLIFO(int processID, int pageNumber) {
  // Check if there are any free frames
  int freeFrameIndex = findFreeFrame();
  if (freeFrameIndex != -1) {
    // If there are free frames, allocate the page to a free frame
    framesInMainMemory[freeFrameIndex].processID = processID;
    framesInMainMemory[freeFrameIndex].pageID = pageNumber;
    framesInMainMemory[freeFrameIndex].isAllocated = true;
    // Update the page table entry of the allocated page
    updatePageTable(processID, pageNumber, true);
    cout << "Page " << pageNumber << " allocated to frame " << freeFrameIndex << " using LIFO algorithm." << endl;
  } else {
    // If there are no free frames, perform page replacement
    int pageToReplace = pageAccessStack.top(); // Get the page accessed least recently
    pageAccessStack.pop(); // Remove the page from the stack
    for (auto &frame : framesInMainMemory) {
      if (frame.pageID == pageToReplace) {
        // Replace the page
        frame.processID = processID;
        frame.pageID = pageNumber;
        // Update the page table entry of the allocated page
        updatePageTable(processID, pageNumber, true);
        cout << "Page " << pageToReplace << " replaced with page " << pageNumber << " using LIFO algorithm." << endl;
        break;
      }
    }
  }
}

// Function to find the most recently used page in the frame table
int findMRUPage() {
  int maxAccessTime = INT_MIN;
  int mruPage = -1;
  for (const auto &frame : framesInMainMemory) {
    if (frame.isAllocated && frame.accessTime > maxAccessTime) {
      maxAccessTime = frame.accessTime;
      mruPage = frame.pageID;
    }
  }
  return mruPage;
}
// Function to handle page replacement using WS algorithm
void replacePageWS(int processID, int pageNumber) {
    // Find the process's working set size
    int workingSetSize = calculateWorkingSetSize(processID);

    // Check if there are any free frames
    int freeFrameIndex = findFreeFrame();
    if (freeFrameIndex != -1) {
        // If there are free frames, allocate the page to a free frame
        framesInMainMemory[freeFrameIndex].processID = processID;
        framesInMainMemory[freeFrameIndex].pageID = pageNumber;
        framesInMainMemory[freeFrameIndex].isAllocated = true;
        // Update the page table entry of the allocated page
        updatePageTable(processID, pageNumber, true);
        cout << "Page " << pageNumber << " allocated to frame " << freeFrameIndex << " using WS algorithm." << endl;
    } else {
        // If there are no free frames, perform page replacement based on working set
        // Check if the current page is within the process's working set
        if (workingSetMap[processID].size() < workingSetSize) {
            // If the working set is not full, allocate the page to a free frame
            for ( auto &frame : framesInMainMemory) {
                if (!frame.isAllocated) {
                    // Allocate the page to a free frame
                    frame.processID = processID;
                    frame.pageID = pageNumber;
                    frame.isAllocated = true;
                    // Update the page table entry of the allocated page
                    updatePageTable(processID, pageNumber, true);
                    cout << "Page " << pageNumber << " allocated to frame " << frame.frameID << " using WS algorithm." << endl;
                    break;
                }
            }
        } else {
            // If the working set is full, perform page replacement based on working set
            int oldestAccessTime = INT_MAX;
            int pageToReplace = -1;
            for (const auto &frame : framesInMainMemory) {
                if (frame.processID == processID && frame.isAllocated && workingSetMap[processID].front() == frame.pageID) {
                    // Find the page with the oldest access time within the working set
                    if (frame.accessTime < oldestAccessTime) {
                        oldestAccessTime = frame.accessTime;
                        pageToReplace = frame.pageID;
                    }
                }
            }
            // Replace the page with the oldest access time within the working set
            for (auto &frame : framesInMainMemory) {
                if (frame.pageID == pageToReplace) {
                    // Replace the page
                    frame.processID = processID;
                    frame.pageID = pageNumber;
                    // Update the page table entry of the allocated page
                    updatePageTable(processID, pageNumber, true);
                    cout << "Page " << pageToReplace << " replaced with page " << pageNumber << " using WS algorithm." << endl;
                    break;
                }
            }
        }
    }
}

void replacePageOPT_X(int processID, int pageNumber, int lookaheadWindowSize) {
    // Initialize a vector to store the indices of future accesses for each page
    vector<int> futureAccess(framesInMainMemory.size(), INT_MAX);

    // Iterate over frames in memory
    for (int i = 0; i < framesInMainMemory.size(); ++i) {
        // If frame is allocated, find the next access of the page in memory
        if (framesInMainMemory[i].isAllocated) {
            for (int j = 0; j < lookaheadWindowSize; ++j) {
                // Find the next access of the page in memory
                for (int k = 0; k < framesInMainMemory.size(); ++k) {
                    if (framesInMainMemory[k].isAllocated && framesInMainMemory[k].pageID == framesInMainMemory[i].pageID && k != i) {
                        futureAccess[i] = j;
                        break;
                    }
                }
            }
        }
    }

    // Find the page with the maximum future access distance
    int maxFutureAccess = INT_MIN;
    int pageToReplace = -1;
    for (int i = 0; i < framesInMainMemory.size(); ++i) {
        if (!framesInMainMemory[i].isAllocated) {
            pageToReplace = i;
            break;
        }
        if (futureAccess[i] > maxFutureAccess) {
            maxFutureAccess = futureAccess[i];
            pageToReplace = i;
        }
    }

    // Replace the page
    framesInMainMemory[pageToReplace].processID = processID;
    framesInMainMemory[pageToReplace].pageID = pageNumber;
    framesInMainMemory[pageToReplace].isAllocated = true;

    // Update the page table entry of the allocated page
    updatePageTable(processID, pageNumber, true);

    cout << "Page " << framesInMainMemory[pageToReplace].pageID << " replaced using OPT-X algorithm." << endl;
}


void *pageFaultHandlerOPT_X(void *arg) {
    while (true) {
      int lookaheadWindowSize;
        // Wait for page fault signal
        sem_wait(&pageFaultSem);

        // Extract the lookahead window size from the argument
        lookaheadWindowSize = *(int*)arg;

        // Handle page fault using OPT-X algorithm
        for (auto &frame : framesInMainMemory) {
            // Modify frame properties here
            //frame.processID = processID;
            //frame.pageID = pageNumber;
            frame.isAllocated = true;
        }

        // Signal disk driver semaphore
        sem_post(&diskDriverSem);
    }
    return NULL;
}

// Function to handle page replacement using LRU algorithm
void replacePageLRU(int processID, int pageNumber) {
    // Find the least recently used page
    int lruPage = findLRUPage();

    // Find the frame containing the LRU page
    for (auto &frame : framesInMainMemory) {
        if (frame.isAllocated && frame.pageID == lruPage) {
            // Replace the LRU page with the new page
            frame.processID = processID;
            frame.pageID = pageNumber;
            // Update access time
            frame.accessTime = getCurrentTime(); // Implement getCurrentTime() function
            // Update page table entry of the replaced page
            updatePageTable(processID, lruPage, false);
            // Update page table entry of the new page
            updatePageTable(processID, pageNumber, true);
            cout << "Page " << lruPage << " replaced with page " << pageNumber << " using LRU algorithm." << endl;
            // Increment page fault count for the current process
            pageFaultCounts[processID]++;
            break;
        }
    }
}


// Function to handle page replacement using MRU algorithm
void replacePageMRU(int processID, int pageNumber) {
  // Find the most recently used page
  int mruPage = findMRUPage();
  // Find the frame containing the MRU page
  for (auto &frame : framesInMainMemory) {
    if (frame.isAllocated && frame.pageID == mruPage) {
      // Replace the MRU page with the new page
      frame.processID = processID;
      frame.pageID = pageNumber;
      // Update access time
      frame.accessTime = getCurrentTime(); // Implement getCurrentTime() function
      // Update page table entry of the replaced page
      updatePageTable(processID, mruPage, false);
      // Update page table entry of the new page
      updatePageTable(processID, pageNumber, true);
      cout << "Page " << mruPage << " replaced with page " << pageNumber << " using MRU algorithm." << endl;
      break;
    }
  }
}

// Function to find a free frame in the main memory
int findFreeFrame() {
  for (int i = 0; i < framesInMainMemory.size(); ++i) {
    if (!framesInMainMemory[i].isAllocated) {
      return i;
    }
  }
  return -1; // No free frames available
}

// Function to handle page replacement using LRU-X algorithm with the specified lookahead window size
void replacePageLRU_X(int processID, int pageNumber, int lookaheadWindowSize) {
    // Implement LRU-X page replacement algorithm here
    // Use the lookahead window size to make replacement decisions
}

void *pageFaultHandlerLRU(void *arg) {
    while (true) {   
        // Wait for page fault signal
        sem_wait(&pageFaultSem);

        // Pop the front of the pageAccessStack to get the pageNumber
        int pageNumber = pageAccessStack.top();
        pageAccessStack.pop();

        // Handle page fault using LRU algorithm
        replacePageLRU(0, pageNumber); // Assuming processID is always 0 for simplicity

        // Signal disk driver semaphore
        sem_post(&diskDriverSem);
    }
    return NULL;
}

// Function to periodically display statistics
void displayStatisticsPeriodically() {
    while (true) {
        // Display statistics
        displayStatistics();
        // Sleep for a specified duration before displaying statistics again
        std::this_thread::sleep_for(std::chrono::seconds(10)); // Adjust the sleep duration as needed
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <input_file>" << endl;
        return 1;
    }

    string filename = argv[1];

    // Read input from file and initialize data structures
    readInputFromFile(filename);
    initializeDataStructures();

    // Initialize semaphores
    initializeSemaphores();

    // Create page fault handler thread
    pthread_t pageFaultThreadMRU;
    pthread_create(&pageFaultThreadMRU, NULL, pageFaultHandlerMRU, NULL);

    pthread_t pageFaultThreadLRU;
    pthread_create(&pageFaultThreadLRU, NULL, pageFaultHandlerLRU, NULL);

    pthread_t pageFaultThreadLIFO;
    pthread_create(&pageFaultThreadLIFO, NULL, pageFaultHandlerLIFO, NULL);

    pthread_t pageFaultThreadWS;
    pthread_create(&pageFaultThreadWS, NULL, pageFaultHandlerWS, NULL);

    pthread_t pageFaultThreadLRU_X;
    pthread_create(&pageFaultThreadLRU_X, NULL, pageFaultHandlerLRU_X, NULL);

    pthread_t pageFaultThreadOPT_X;
    pthread_create(&pageFaultThreadOPT_X, NULL, pageFaultHandlerOPT_X, NULL);

    // Create disk driver thread
    pthread_t diskDriverThread;
    pthread_create(&diskDriverThread, NULL, diskDriver, NULL);

    // Create a thread to display statistics periodically
    std::thread statisticsThread(displayStatisticsPeriodically);



    // Simulate page accesses and disk I/O requests
    while (true) {
        // Simulate page accesses
        pageAccessHandler();

        // Simulate disk I/O requests
        handleDiskIORequest();

        // Sleep for a short duration
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Adjust as needed
    }

    // Wait for the threads to finish execution

    pthread_join(pageFaultThreadMRU, NULL);
    pthread_join(pageFaultThreadLRU, NULL);
    pthread_join(pageFaultThreadLIFO, NULL);
    pthread_join(pageFaultThreadWS, NULL);
    pthread_join(pageFaultThreadLRU_X, NULL);
    pthread_join(pageFaultThreadOPT_X, NULL);
    pthread_join(diskDriverThread, NULL);
    statisticsThread.join();

    return 0;
}