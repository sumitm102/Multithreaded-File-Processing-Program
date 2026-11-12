// CSCI 340 - Project 1

#include <iostream>
#include <string>
#include <unordered_map>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <cctype>
#include <cerrno>

using namespace std;

// Structure for each item in the buffer
typedef struct {
    char *data;
    int bytes;
} item;

// Global variables shared by the threads

item *buffer; // Shared circular buffer

int bufferSize;
int chunkSize;

int in = 0; // Index for the next free position in the buffer
int out = 0; // Index for the next full position in the buffer

bool producerDone = false; // Boolean flag indicating whether the producer has finished producing
bool readFailed = false; // Boolean flag indicating whether a read operation failed

int producedChunkCount = 0;
int consumedChunkCount = 0;

// Hash table for character frequencies
unordered_map<char, int> characterCount;

const char *inputFile;

// Mutex protects access to the shared buffer and variables
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

// Prints messages using the POSIX write() system call
void printMessage(string message) {
    int written = 0;
    int msgLength = (int)message.length();

    while (written < msgLength) {
        int n = (int)write(STDOUT_FILENO, message.c_str() + written, msgLength - written);

        // Handle interrupted system call
        if (n < 0 && errno == EINTR)
            continue;

        // Handle write error
        if (n <= 0)
            return;

        written += n;
    }
}

// Converts newline and tab characters into readable text
string displayChunk(char *data, int length) {
    string result = "";

    for (int i = 0; i < length; i++) {
        unsigned char c = (unsigned char)data[i];

        if (c == '\n')
            result += "\\n";
        else if (c == '\t')
            result += "\\t";
        else if (isprint(c))
            result += (char)c;
        else
            result += "?";
    }

    return result;
}

// Producer thread functionality
void *producer(void *param) {

    // Opening the input file
    int fd = open(inputFile, O_RDONLY);

    // Checking if the file was opened successfully
    if (fd == -1) {
        printMessage("Producer: Cannot open input file\n");

        pthread_mutex_lock(&mutex);
        readFailed = true;
        producerDone = true;
        pthread_mutex_unlock(&mutex);

        return NULL;
    }

    // Temporary item for storing chunks read from the input file
    item nextProduced;
    nextProduced.data = new char[chunkSize];

    while (true) {

        // Reading up to chunk size bytes
        int n = (int)read(fd, nextProduced.data, chunkSize);

        // Handling interrupted system call
        if (n < 0 && errno == EINTR)
            continue;

        // Handling read error
        if (n < 0) {
            printMessage("Producer: Error reading input file\n");

            pthread_mutex_lock(&mutex);
            readFailed = true;
            pthread_mutex_unlock(&mutex);

            break;
        }

        // End of file
        if (n == 0)
            break;

        nextProduced.bytes = n;

        int availableSlot;

        // Waiting when the buffer is full
        while (true) {

            pthread_mutex_lock(&mutex);

            // Checking if the next buffer position is available
            if ((in + 1) % bufferSize != out) {

                availableSlot = in;

                // Copying the produced item into the shared buffer
                for (int i = 0; i < nextProduced.bytes; i++) {
                    buffer[availableSlot].data[i] = nextProduced.data[i];
                }

                // Storing the actual number of bytes
                buffer[availableSlot].bytes = nextProduced.bytes;

                // Moving in index to the next circular buffer position
                in = (in + 1) % bufferSize;

                producedChunkCount++;

                pthread_mutex_unlock(&mutex);
                break;
            }

            pthread_mutex_unlock(&mutex);
        }

        // Displaying the produced chunk
        printMessage("In  Buffer[" + to_string(availableSlot) + "] Producer: " + displayChunk(nextProduced.data, nextProduced.bytes) + "\n");
    }

    delete[] nextProduced.data;
    close(fd);

    // Telling the consumer that production has finished
    pthread_mutex_lock(&mutex);
    producerDone = true;
    pthread_mutex_unlock(&mutex);

    printMessage("Producer: # chunks produced = " + to_string(producedChunkCount) + "\n");
    printMessage("Producer terminated\n");

    return NULL;
}

// Consumer thread functionality
void *consumer(void *param) {

    // Temporary item to store chunks consumed from the buffer
    item nextConsumed;
    nextConsumed.data = new char[chunkSize];

    while (true) {

        int availableSlot = -1;

        // Waiting when the buffer is empty
        while (true) {

            pthread_mutex_lock(&mutex);

            // Checking whether a chunk is available
            if (out != in) {

                availableSlot = out;
                nextConsumed.bytes = buffer[availableSlot].bytes;

                // Copying the chunk out of the shared buffer
                for (int i = 0; i < nextConsumed.bytes; i++) {
                    nextConsumed.data[i] = buffer[availableSlot].data[i];
                }

                // Moving out index to the next circular buffer position
                out = (out + 1) % bufferSize;

                consumedChunkCount++;

                pthread_mutex_unlock(&mutex);
                break;
            }

            // Checking whether the producer has finished
            bool finished = producerDone;

            pthread_mutex_unlock(&mutex);

            if (finished) {

                delete[] nextConsumed.data;

                printMessage("Consumer: # chunks consumed = " + to_string(consumedChunkCount) + "\n");
                printMessage("Consumer terminated\n");

                return NULL;
            }
        }

        // Displaying the consumed chunk
        printMessage("Out Buffer[" + to_string(availableSlot) + "] Consumer: " + displayChunk(nextConsumed.data, nextConsumed.bytes) + "\n");

        // Counting printable characters
        for (int i = 0; i < nextConsumed.bytes; i++) {
            unsigned char c = (unsigned char)nextConsumed.data[i];

            if (isprint(c)) {

                // Converting uppercase letters to lowercase
                char character = (char)tolower(c);
                characterCount[character]++;
            }
        }
    }
}

// Main thread
int main(int argc, char *argv[]) {

    // Checking command-line arguments
    if (argc != 4) {
        printMessage("Format: ./Mandal_24182260.exe <src_file> <buffer_size> <chunk_size>\n");
        return 1;
    }

    inputFile = argv[1];

    // Converting command-line arguments to integers
    bufferSize = atoi(argv[2]);
    chunkSize = atoi(argv[3]);

    if (bufferSize < 2 || chunkSize < 1) {
        printMessage("Error: Invalid buffer size or chunk size\n");
        return 1;
    }

    // Creating the shared circular buffer
    buffer = new item[bufferSize];

    for (int i = 0; i < bufferSize; i++) {
        buffer[i].data = new char[chunkSize];
        buffer[i].bytes = 0;
    }

    printMessage("[Source: " + string(inputFile) + "] [Buffer Size: " + to_string(bufferSize) +
    "] [Chunk Size: " + to_string(chunkSize) + "]\n");

    // Creating the producer and consumer threads
    pthread_t producerThread;
    pthread_t consumerThread;

    int result = pthread_create(&consumerThread, NULL, consumer, NULL);

    if (result != 0) {
        printMessage("Error creating consumer thread\n");
        return 2;
    }

    result = pthread_create(&producerThread, NULL, producer, NULL);

    if (result != 0) {
        printMessage("Error creating producer thread\n");

        pthread_mutex_lock(&mutex);
        producerDone = true;
        pthread_mutex_unlock(&mutex);

        pthread_join(consumerThread, NULL);
        return 2;
    }

    // Waiting for both threads to finish
    pthread_join(producerThread, NULL);
    pthread_join(consumerThread, NULL);

    
    printMessage("\nCharacter  Count\n");
    printMessage("----------------\n");

    // Printing the final character counts
    for (int i = 0; i < 256; i++) {
    char character = (char)i;

    if (characterCount.find(character) != characterCount.end()) {

        string display = string(1, character) + string(10, ' ');

        if (character == ' ')
            display = "Space" + string(6, ' ');

        printMessage(display + to_string(characterCount[character]) + "\n");
        }
    }

    printMessage("\nMain thread terminated\n\n");

    // Freeing dynamically allocated memory
    for (int i = 0; i < bufferSize; i++) {
        delete[] buffer[i].data;
    }

    delete[] buffer;

    // Destroying the mutex
    pthread_mutex_destroy(&mutex);

    if (readFailed)
        return 3;

    return 0;
}
